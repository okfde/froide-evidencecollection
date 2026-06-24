    (function () {
        var prefersReducedMotion = window.matchMedia
            && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

        // Server-rendered i18n strings, read from data-* on the page root so
        // this file can stay a static (non-templated) asset under the strict
        // host CSP (no inline scripts allowed).
        var pageRoot = document.querySelector('.topic-cloud-page');
        var I18N = (pageRoot && pageRoot.dataset) || {};

        // ── Collision-relaxation layout ──────────────────────────────
        // The server projects each post's UMAP coordinate to a cx/cy, so
        // dots pile up wherever the data is dense. Instead of drifting,
        // we run a small force layout: every dot is sprung toward its
        // UMAP anchor (preserving cluster structure) while being pushed
        // off any neighbour it overlaps, so the final, *static* layout
        // spreads dense clusters out without dots covering each other.
        //
        // anchorRegistry maps each circle → its projected {ax, ay}. We
        // always relax from the anchors (not the current, already-relaxed
        // positions) so the result is deterministic and a dot lands in the
        // same place regardless of which filter-driven neighbours are
        // present — no cumulative drift across filter changes.
        var anchorRegistry = new Map();
        var COLLIDE_PAD = 1.0;    // min gap between dot edges (viewBox units)
        var RELAX_ITERS = 70;     // relaxation passes
        var ANCHOR_PULL = 0.09;   // spring strength back toward the UMAP anchor

        function registerCircle(circle) {
            if (anchorRegistry.has(circle)) return;
            var ax = parseFloat(circle.getAttribute('cx'));
            var ay = parseFloat(circle.getAttribute('cy'));
            if (!isFinite(ax) || !isFinite(ay)) return;
            anchorRegistry.set(circle, { ax: ax, ay: ay });
        }

        function unregisterCircle(circle) {
            anchorRegistry.delete(circle);
        }

        // Push apart every pair of overlapping dots and spring each back
        // toward its anchor, repeated until the layout settles. A uniform
        // spatial grid keeps collision checks local (only the 3×3 cell
        // neighbourhood) so this stays cheap even at a couple thousand dots.
        function relaxLayout() {
            if (!zoomState) return;
            var svg = zoomState.svg;
            var full = zoomState.full;
            var nodes = [];
            Array.prototype.forEach.call(
                svg.querySelectorAll('circle[data-href]'),
                function (c) {
                    if (c.dataset.removing === '1') return;
                    var a = anchorRegistry.get(c);
                    if (!a) return;
                    nodes.push({
                        c: c,
                        x: a.ax, y: a.ay,    // start each relax from the anchor
                        ax: a.ax, ay: a.ay,
                        r: parseFloat(c.getAttribute('r')) || 2,
                    });
                }
            );
            var n = nodes.length;
            if (!n) return;

            var maxR = 0;
            for (var i = 0; i < n; i++) if (nodes[i].r > maxR) maxR = nodes[i].r;
            var cell = (maxR + COLLIDE_PAD) * 2 || 8;
            var cols = Math.max(1, Math.ceil(full.w / cell));
            var rows = Math.max(1, Math.ceil(full.h / cell));

            function colOf(x) {
                return clamp(Math.floor((x - full.x) / cell), 0, cols - 1);
            }
            function rowOf(y) {
                return clamp(Math.floor((y - full.y) / cell), 0, rows - 1);
            }

            for (var iter = 0; iter < RELAX_ITERS; iter++) {
                // Bucket every node into the grid by its current position.
                var grid = Object.create(null);
                for (var i = 0; i < n; i++) {
                    var k = rowOf(nodes[i].y) * cols + colOf(nodes[i].x);
                    (grid[k] || (grid[k] = [])).push(i);
                }
                // Resolve collisions against the 3×3 neighbourhood, each
                // overlapping pair pushed apart by half the overlap.
                for (var i = 0; i < n; i++) {
                    var a = nodes[i];
                    var ci = colOf(a.x), cj = rowOf(a.y);
                    for (var gx = ci - 1; gx <= ci + 1; gx++) {
                        if (gx < 0 || gx >= cols) continue;
                        for (var gy = cj - 1; gy <= cj + 1; gy++) {
                            if (gy < 0 || gy >= rows) continue;
                            var bucket = grid[gy * cols + gx];
                            if (!bucket) continue;
                            for (var bi = 0; bi < bucket.length; bi++) {
                                var j = bucket[bi];
                                if (j <= i) continue;   // visit each pair once
                                var b = nodes[j];
                                var dx = b.x - a.x, dy = b.y - a.y;
                                var minD = a.r + b.r + COLLIDE_PAD;
                                var d2 = dx * dx + dy * dy;
                                if (d2 >= minD * minD) continue;
                                var d = Math.sqrt(d2);
                                if (d < 0.01) {   // coincident — nudge apart
                                    dx = Math.random() - 0.5;
                                    dy = Math.random() - 0.5;
                                    d = Math.sqrt(dx * dx + dy * dy) || 0.01;
                                }
                                var push = (minD - d) / 2;
                                var ux = dx / d, uy = dy / d;
                                a.x -= ux * push; a.y -= uy * push;
                                b.x += ux * push; b.y += uy * push;
                            }
                        }
                    }
                }
                // Spring back toward the UMAP anchor so clusters hold their
                // shape instead of the whole cloud inflating outward.
                for (var i = 0; i < n; i++) {
                    var a = nodes[i];
                    a.x += (a.ax - a.x) * ANCHOR_PULL;
                    a.y += (a.ay - a.y) * ANCHOR_PULL;
                }
            }

            // Clamp inside the canvas and write the settled positions back.
            var pad = 2;
            for (var i = 0; i < n; i++) {
                var a = nodes[i];
                a.c.setAttribute(
                    'cx', clamp(a.x, full.x + pad, full.x + full.w - pad).toFixed(1)
                );
                a.c.setAttribute(
                    'cy', clamp(a.y, full.y + pad, full.y + full.h - pad).toFixed(1)
                );
            }
        }

        // ── Zoom / pan ──────────────────────────────────────────────
        // We mutate the SVG's viewBox to zoom/pan. Wheel zooms around
        // the cursor; left-drag pans; a Reset button (visible only
        // while zoomed) restores the full extent. Filter changes call
        // fitToCircles so the result fills the frame automatically.
        var zoomState = null;
        // Actor highlight: the id of the actor whose dots are emphasised in the
        // cloud (null = none). Set by the "Actors in view" panel; re-applied
        // after every filter swap since merged-in dots arrive without the mark.
        var activeActorId = null;
        var ZOOM_MIN_FRAC = 1 / 30;   // closest zoom: ~30x in
        var ZOOM_PAD = 24;            // viewBox units of padding on auto-fit
        var ZOOM_ANIM_MS = 400;
        var ZOOM_BTN_FACTOR = 0.7;    // each +/- click scales the view by this
        var ZOOM_BTN_ANIM_MS = 200;

        function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

        function parseViewBox(svg) {
            var attr = (svg.getAttribute('viewBox') || '').split(/\s+/).map(parseFloat);
            if (attr.length !== 4 || attr.some(function (n) { return !isFinite(n); })) {
                return null;
            }
            return { x: attr[0], y: attr[1], w: attr[2], h: attr[3] };
        }

        function applyView() {
            if (!zoomState) return;
            var v = zoomState.view;
            zoomState.svg.setAttribute(
                'viewBox', v.x + ' ' + v.y + ' ' + v.w + ' ' + v.h
            );
            var zoomed = v.w < zoomState.full.w - 0.5
                || v.h < zoomState.full.h - 0.5
                || Math.abs(v.x - zoomState.full.x) > 0.5
                || Math.abs(v.y - zoomState.full.y) > 0.5;
            if (zoomState.resetBtn) zoomState.resetBtn.hidden = !zoomed;
            updateZoomButtons();
        }

        function clampView() {
            var v = zoomState.view;
            var f = zoomState.full;
            if (v.w >= f.w) v.x = f.x + (f.w - v.w) / 2;
            else v.x = clamp(v.x, f.x, f.x + f.w - v.w);
            if (v.h >= f.h) v.y = f.y + (f.h - v.h) / 2;
            else v.y = clamp(v.y, f.y, f.y + f.h - v.h);
        }

        function cancelViewAnim() {
            if (zoomState && zoomState.animFrame) {
                cancelAnimationFrame(zoomState.animFrame);
                zoomState.animFrame = null;
            }
        }

        function animateView(target, duration) {
            if (!zoomState) return;
            cancelViewAnim();
            if (prefersReducedMotion) {
                zoomState.view = { x: target.x, y: target.y, w: target.w, h: target.h };
                applyView();
                return;
            }
            var start = {
                x: zoomState.view.x, y: zoomState.view.y,
                w: zoomState.view.w, h: zoomState.view.h,
            };
            var t0 = performance.now();
            var ms = duration || ZOOM_ANIM_MS;
            function step(now) {
                var t = Math.min(1, (now - t0) / ms);
                var e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
                zoomState.view.x = start.x + (target.x - start.x) * e;
                zoomState.view.y = start.y + (target.y - start.y) * e;
                zoomState.view.w = start.w + (target.w - start.w) * e;
                zoomState.view.h = start.h + (target.h - start.h) * e;
                applyView();
                if (t < 1) zoomState.animFrame = requestAnimationFrame(step);
                else zoomState.animFrame = null;
            }
            zoomState.animFrame = requestAnimationFrame(step);
        }

        function fitToCircles(animate) {
            if (!zoomState) return;
            var svg = zoomState.svg;
            var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            var count = 0;
            Array.prototype.forEach.call(
                svg.querySelectorAll('circle[data-href]'),
                function (c) {
                    if (c.dataset.removing === '1') return;
                    var cx = parseFloat(c.getAttribute('cx'));
                    var cy = parseFloat(c.getAttribute('cy'));
                    var r = parseFloat(c.getAttribute('r')) || 0;
                    if (!isFinite(cx) || !isFinite(cy)) return;
                    if (cx - r < minX) minX = cx - r;
                    if (cy - r < minY) minY = cy - r;
                    if (cx + r > maxX) maxX = cx + r;
                    if (cy + r > maxY) maxY = cy + r;
                    count++;
                }
            );
            var f = zoomState.full;
            var target;
            if (count === 0) {
                target = { x: f.x, y: f.y, w: f.w, h: f.h };
            } else {
                minX -= ZOOM_PAD; minY -= ZOOM_PAD;
                maxX += ZOOM_PAD; maxY += ZOOM_PAD;
                var w = maxX - minX;
                var h = maxY - minY;
                // Match the full viewBox aspect so the SVG isn't letterboxed.
                var aspect = f.w / f.h;
                if (w / h > aspect) {
                    var newH = w / aspect;
                    minY -= (newH - h) / 2;
                    h = newH;
                } else {
                    var newW = h * aspect;
                    minX -= (newW - w) / 2;
                    w = newW;
                }
                if (w >= f.w) {
                    target = { x: f.x, y: f.y, w: f.w, h: f.h };
                } else {
                    target = {
                        x: clamp(minX, f.x, f.x + f.w - w),
                        y: clamp(minY, f.y, f.y + f.h - h),
                        w: w,
                        h: h,
                    };
                }
            }
            if (animate) animateView(target);
            else {
                cancelViewAnim();
                zoomState.view = target;
                applyView();
            }
        }

        // Zoom in (zoomIn=true) or out by one step, animating around the
        // centre of the current view. Used by the +/- buttons; the wheel no
        // longer zooms so the page scrolls normally over the cloud.
        function zoomStep(zoomIn) {
            if (!zoomState) return;
            cancelViewAnim();
            var v = zoomState.view;
            var f = zoomState.full;
            var factor = zoomIn ? ZOOM_BTN_FACTOR : 1 / ZOOM_BTN_FACTOR;
            var minW = f.w * ZOOM_MIN_FRAC;
            var newW = clamp(v.w * factor, minW, f.w);
            var ratio = newW / v.w;
            if (ratio === 1) return;
            var cx = v.x + v.w / 2;
            var cy = v.y + v.h / 2;
            var target = {
                x: cx - (cx - v.x) * ratio,
                y: cy - (cy - v.y) * ratio,
                w: v.w * ratio,
                h: v.h * ratio,
            };
            if (target.w >= f.w) {
                target = { x: f.x, y: f.y, w: f.w, h: f.h };
            } else {
                target.x = clamp(target.x, f.x, f.x + f.w - target.w);
                target.y = clamp(target.y, f.y, f.y + f.h - target.h);
            }
            animateView(target, ZOOM_BTN_ANIM_MS);
        }

        // Disable + at max zoom-in and − at full extent. Called from applyView,
        // so the buttons track every view change (clicks, reset, auto-fit).
        function updateZoomButtons() {
            if (!zoomState || !zoomState.zoomInBtn) return;
            var f = zoomState.full;
            var w = zoomState.view.w;
            zoomState.zoomInBtn.disabled = w <= f.w * ZOOM_MIN_FRAC + 0.5;
            zoomState.zoomOutBtn.disabled = w >= f.w - 0.5;
        }

        function setupZoom(figure, svg) {
            var full = parseViewBox(svg) || { x: 0, y: 0, w: 800, h: 480 };
            zoomState = {
                figure: figure,
                svg: svg,
                full: full,
                view: { x: full.x, y: full.y, w: full.w, h: full.h },
                animFrame: null,
                resetBtn: null,
                zoomInBtn: null,
                zoomOutBtn: null,
                dragSuppressClick: false,
            };
            svg.classList.add('is-pannable');

            var resetBtn = document.createElement('button');
            resetBtn.type = 'button';
            resetBtn.className = 'topic-cloud-zoom-reset';
            resetBtn.hidden = true;
            resetBtn.textContent = I18N.i18nResetZoom || 'Reset zoom';
            resetBtn.addEventListener('click', function () {
                animateView(zoomState.full);
            });
            figure.appendChild(resetBtn);
            zoomState.resetBtn = resetBtn;

            // Dedicated +/- buttons replace wheel-zoom: scrolling over the
            // cloud now pans the page as usual instead of zooming.
            var controls = document.createElement('div');
            controls.className = 'topic-cloud-zoom-controls';
            var zoomInBtn = document.createElement('button');
            zoomInBtn.type = 'button';
            zoomInBtn.textContent = '+';
            zoomInBtn.setAttribute(
                'aria-label', I18N.i18nZoomIn || 'Zoom in'
            );
            zoomInBtn.title = I18N.i18nZoomIn || 'Zoom in';
            zoomInBtn.addEventListener('click', function () { zoomStep(true); });
            var zoomOutBtn = document.createElement('button');
            zoomOutBtn.type = 'button';
            zoomOutBtn.textContent = '−';
            zoomOutBtn.setAttribute(
                'aria-label', I18N.i18nZoomOut || 'Zoom out'
            );
            zoomOutBtn.title = I18N.i18nZoomOut || 'Zoom out';
            zoomOutBtn.addEventListener('click', function () { zoomStep(false); });
            controls.appendChild(zoomInBtn);
            controls.appendChild(zoomOutBtn);
            figure.appendChild(controls);
            zoomState.zoomInBtn = zoomInBtn;
            zoomState.zoomOutBtn = zoomOutBtn;
            updateZoomButtons();

            var drag = null;
            function onMove(ev) {
                if (!drag) return;
                var dx = ev.clientX - drag.x0;
                var dy = ev.clientY - drag.y0;
                if (!drag.moved && (Math.abs(dx) > 3 || Math.abs(dy) > 3)) {
                    drag.moved = true;
                    svg.classList.add('is-panning');
                }
                var rect = svg.getBoundingClientRect();
                var v = zoomState.view;
                v.x = drag.vx0 - dx / rect.width * v.w;
                v.y = drag.vy0 - dy / rect.height * v.h;
                clampView();
                applyView();
            }
            function onUp() {
                if (drag && drag.moved) zoomState.dragSuppressClick = true;
                drag = null;
                svg.classList.remove('is-panning');
                window.removeEventListener('mousemove', onMove);
                window.removeEventListener('mouseup', onUp);
            }
            svg.addEventListener('mousedown', function (ev) {
                if (ev.button !== 0) return;
                cancelViewAnim();
                drag = {
                    x0: ev.clientX, y0: ev.clientY,
                    vx0: zoomState.view.x, vy0: zoomState.view.y,
                    moved: false,
                };
                window.addEventListener('mousemove', onMove);
                window.addEventListener('mouseup', onUp);
            });
        }

        // Re-runnable: the filter form swaps #topic-cloud-results via fetch
        // and then calls this again to bind the freshly inserted figure.
        function bindCloud() {
            var figure = document.querySelector('.topic-cloud-svg');
            if (!figure) return;
            var svg = figure.querySelector('svg');
            var tooltip = figure.querySelector('.topic-cloud-tooltip');
            if (!svg) return;

            setupZoom(figure, svg);
            // Anchor every dot at its projected position, then relax so
            // overlapping dots spread apart into a static, non-overlapping
            // layout (relaxLayout needs zoomState, hence after setupZoom).
            Array.prototype.forEach.call(
                svg.querySelectorAll('circle[data-href]'),
                registerCircle
            );
            relaxLayout();
            // Reflect server-rendered filters in the initial framing.
            fitToCircles(false);

            svg.addEventListener('click', function (ev) {
                if (zoomState && zoomState.dragSuppressClick) {
                    zoomState.dragSuppressClick = false;
                    return;
                }
                var target = ev.target.closest('circle[data-href]');
                if (!target) return;
                window.open(target.getAttribute('data-href'), '_blank', 'noopener');
            });

            if (!tooltip) return;

            var sourceEl = tooltip.querySelector('.topic-cloud-tooltip__source');
            var metaEl = tooltip.querySelector('.topic-cloud-tooltip__meta');

            // One "Label: value" row for the tooltip's metadata block, built
            // with DOM nodes (not innerHTML) so the values stay text-safe.
            function metaRow(label, value) {
                var row = document.createElement('div');
                row.className = 'topic-cloud-tooltip__meta-row';
                var labelEl = document.createElement('span');
                labelEl.className = 'topic-cloud-tooltip__meta-label';
                labelEl.textContent = label + ':';
                row.appendChild(labelEl);
                row.appendChild(document.createTextNode(value));
                return row;
            }

            function fillTooltip(circle) {
                var platform = circle.getAttribute('data-platform') || '';
                var username = circle.getAttribute('data-username') || '';
                var postedOn = circle.getAttribute('data-posted-on') || '';
                var originators = circle.getAttribute('data-originators') || '';
                var chapters = circle.getAttribute('data-chapters') || '';

                // Header line: the table's Platform / Account / Date columns.
                var source = platform;
                if (username) source += (source ? ' ' : '') + '@' + username;
                if (postedOn) source += (source ? ' · ' : '') + postedOn;
                sourceEl.textContent = source;

                // Remaining table columns (originator with Verband, chapters)
                // as labelled rows — no text snippet.
                metaEl.textContent = '';
                if (originators) {
                    metaEl.appendChild(metaRow(I18N.i18nOriginator || 'Originator', originators));
                }
                if (chapters) {
                    metaEl.appendChild(metaRow(I18N.i18nChapters || 'Chapters', chapters));
                }
            }

            function positionTooltip(ev) {
                var rect = figure.getBoundingClientRect();
                var x = ev.clientX - rect.left;
                var y = ev.clientY - rect.top;
                var margin = 12;
                tooltip.hidden = false;
                var tw = tooltip.offsetWidth;
                var th = tooltip.offsetHeight;
                var maxLeft = rect.width - tw - 2;
                var left = Math.min(Math.max(0, x + margin), Math.max(0, maxLeft));
                var top = y + margin;
                if (top + th > rect.height && y - margin - th >= 0) {
                    top = y - margin - th;
                }
                tooltip.style.left = left + 'px';
                tooltip.style.top = top + 'px';
            }

            svg.addEventListener('mouseover', function (ev) {
                var target = ev.target.closest('circle[data-href]');
                if (!target) return;
                fillTooltip(target);
                positionTooltip(ev);
            });
            svg.addEventListener('mousemove', function (ev) {
                if (tooltip.hidden) return;
                var target = ev.target.closest('circle[data-href]');
                if (!target) {
                    tooltip.hidden = true;
                    return;
                }
                positionTooltip(ev);
            });
            svg.addEventListener('mouseleave', function () {
                tooltip.hidden = true;
            });
        }

        bindCloud();

        // ── View tabs: scatter cloud ⇆ tabular list ───────────────────
        // Pure client-side: clicking a tab flips a class on .topic-cloud-page
        // (read by CSS to show one view, hide the other) and moves the active
        // styling. The choice lives outside #topic-cloud-results, so it persists
        // across filter swaps. The table content itself is server-rendered and
        // swapped like the outline, so it stays in sync with the filters.
        (function () {
            var page = document.querySelector('.topic-cloud-page');
            var tabs = document.querySelectorAll('.topic-cloud-tab');
            if (!page || !tabs.length) return;
            function setView(view) {
                page.classList.toggle('topic-view-table', view === 'table');
                page.classList.toggle('topic-view-cloud', view === 'cloud');
                Array.prototype.forEach.call(tabs, function (tab) {
                    var on = tab.getAttribute('data-view') === view;
                    tab.classList.toggle('is-active', on);
                    tab.setAttribute('aria-selected', on ? 'true' : 'false');
                });
                // Returning to the cloud after it was hidden: re-frame the dots
                // so the SVG fills the (now measurable) frame again.
                if (view === 'cloud') fitToCircles(false);
            }
            Array.prototype.forEach.call(tabs, function (tab) {
                tab.addEventListener('click', function () {
                    setView(tab.getAttribute('data-view'));
                });
            });
        })();

        // ── Actor combobox: button opens a popup with a search input ──
        // and a filterable list. The hidden input carries the submitted
        // PK; selecting an option dispatches a `change` event on it so
        // the live-filter pipeline below picks it up.
        var actorCombobox = (function () {
            var button = document.getElementById('tf-actor-button');
            var popup = document.getElementById('tf-actor-popup');
            var search = document.getElementById('tf-actor-search');
            var list = document.getElementById('tf-actor-options');
            var hidden = document.getElementById('tf-actor');
            var label = document.getElementById('tf-actor-button-label');
            if (!button || !popup || !list || !hidden || !label) return null;

            var anyText = list.querySelector('li[data-value=""]');
            var anyLabel = anyText ? anyText.textContent.trim() : '';

            function applyFilter(q) {
                q = (q || '').trim().toLowerCase();
                Array.prototype.forEach.call(list.children, function (li) {
                    if (li.getAttribute('data-value') === '') {
                        li.hidden = false;
                        return;
                    }
                    li.hidden = q
                        && li.textContent.trim().toLowerCase().indexOf(q) === -1;
                });
            }
            function open() {
                popup.hidden = false;
                button.setAttribute('aria-expanded', 'true');
                if (search) {
                    search.value = '';
                    applyFilter('');
                    search.focus();
                }
                // Scroll the selected option into view so the user can
                // see where they are inside a long list.
                var sel = list.querySelector('li[aria-selected="true"]');
                if (sel && sel.scrollIntoView) {
                    sel.scrollIntoView({block: 'nearest'});
                }
            }
            function close() {
                popup.hidden = true;
                button.setAttribute('aria-expanded', 'false');
            }
            function select(li) {
                var value = li.getAttribute('data-value') || '';
                Array.prototype.forEach.call(list.children, function (other) {
                    other.removeAttribute('aria-selected');
                });
                li.setAttribute('aria-selected', 'true');
                hidden.value = value;
                label.textContent = value ? li.textContent.trim() : anyLabel;
                close();
                button.focus();
                hidden.dispatchEvent(new Event('change', {bubbles: true}));
            }

            button.addEventListener('click', function (ev) {
                ev.stopPropagation();
                if (popup.hidden) open();
                else close();
            });
            if (search) {
                search.addEventListener('input', function () {
                    applyFilter(search.value);
                });
                search.addEventListener('keydown', function (ev) {
                    if (ev.key === 'Escape') {
                        ev.preventDefault();
                        close();
                        button.focus();
                    } else if (ev.key === 'Enter') {
                        ev.preventDefault();
                        var visible = list.querySelector('li:not([hidden])');
                        if (visible) select(visible);
                    }
                });
            }
            list.addEventListener('click', function (ev) {
                var li = ev.target.closest('li[data-value]');
                if (!li) return;
                select(li);
            });
            // Close on outside click + Escape outside the popup.
            document.addEventListener('click', function (ev) {
                if (popup.hidden) return;
                if (popup.contains(ev.target) || button.contains(ev.target)) return;
                close();
            });
            document.addEventListener('keydown', function (ev) {
                if (ev.key === 'Escape' && !popup.hidden) {
                    close();
                    button.focus();
                }
            });

            return {
                reset: function () {
                    Array.prototype.forEach.call(list.children, function (other) {
                        other.removeAttribute('aria-selected');
                    });
                    if (anyText) anyText.setAttribute('aria-selected', 'true');
                    hidden.value = '';
                    label.textContent = anyLabel;
                },
            };
        })();

        // ── Year range slider ─────────────────────────────────────────
        // Two overlaid range inputs select a [low, high] span of whole
        // years; clicking a tick collapses both onto that single year. The
        // handles write Jan-1 / Dec-31 into the hidden #tf-after / #tf-before
        // date inputs (cleared when the span covers the full extent, so "all
        // years" is an empty, unfiltered selection), then dispatch a `change`
        // so the live-filter pipeline below submits — the same path the other
        // controls use.
        var yearSlider = (function () {
            var root = document.querySelector('.topic-year-slider');
            if (!root) return null;
            var low = document.getElementById('tf-year-low');
            var high = document.getElementById('tf-year-high');
            var rangeBar = document.getElementById('tf-year-range');
            var ticksBox = document.getElementById('tf-year-ticks');
            var readout = document.getElementById('tf-year-readout');
            var afterInput = document.getElementById('tf-after');
            var beforeInput = document.getElementById('tf-before');
            if (!low || !high || !rangeBar || !readout) return null;

            var yearMin = parseInt(root.dataset.yearMin, 10);
            var yearMax = parseInt(root.dataset.yearMax, 10);
            var span = yearMax - yearMin;
            var baseLabel = readout.textContent;   // the static "Years" label

            function pct(year) {
                return span ? ((year - yearMin) / span) * 100 : 0;
            }

            // Paint the coloured range bar + readout from the handle values.
            function paint() {
                var lo = parseInt(low.value, 10);
                var hi = parseInt(high.value, 10);
                rangeBar.style.left = pct(lo) + '%';
                rangeBar.style.width = (pct(hi) - pct(lo)) + '%';
                readout.textContent =
                    baseLabel + ': ' + (lo === hi ? String(lo) : lo + '–' + hi);
            }

            // Mirror the handles into the hidden date inputs. A full-extent
            // span clears them, so the request carries no date filter at all.
            function writeInputs() {
                var lo = parseInt(low.value, 10);
                var hi = parseInt(high.value, 10);
                var full = (lo === yearMin && hi === yearMax);
                if (afterInput) afterInput.value = full ? '' : lo + '-01-01';
                if (beforeInput) beforeInput.value = full ? '' : hi + '-12-31';
            }

            // Keep low <= high as a handle is dragged past its partner, and
            // keep whichever thumb can still travel inward on top so a pair
            // sitting on the same year stays grabbable.
            function clampOrder(moved) {
                var lo = parseInt(low.value, 10);
                var hi = parseInt(high.value, 10);
                if (lo > hi) {
                    if (moved === low) high.value = lo;
                    else low.value = hi;
                }
                var highAtEnd = parseInt(high.value, 10) === yearMax;
                low.style.zIndex = highAtEnd ? 4 : 3;
                high.style.zIndex = highAtEnd ? 3 : 4;
            }

            function set(lo, hi) {
                low.value = lo;
                high.value = hi;
                clampOrder(null);
                paint();
                writeInputs();
            }

            function submit() {
                // Bubbling change → the form's htmx `change` trigger.
                low.dispatchEvent(new Event('change', {bubbles: true}));
            }

            // Clickable year ticks: clicking one selects that single year.
            if (ticksBox) {
                for (var y = yearMin; y <= yearMax; y++) {
                    var tick = document.createElement('span');
                    tick.className = 'topic-year-slider__tick';
                    tick.style.left = pct(y) + '%';
                    tick.dataset.year = y;
                    tick.title = y;
                    ticksBox.appendChild(tick);
                }
                ticksBox.addEventListener('click', function (ev) {
                    var t = ev.target.closest('.topic-year-slider__tick');
                    if (!t) return;
                    var yr = parseInt(t.dataset.year, 10);
                    set(yr, yr);
                    submit();
                });
            }

            [low, high].forEach(function (input) {
                // Live feedback while dragging; the native change on release
                // bubbles to htmx, so no explicit submit here.
                input.addEventListener('input', function () {
                    clampOrder(input);
                    paint();
                    writeInputs();
                });
                input.addEventListener('change', function () {
                    clampOrder(input);
                    paint();
                    writeInputs();
                });
            });

            // Initial paint from the server-rendered handle positions.
            set(
                parseInt(root.dataset.yearFrom, 10) || yearMin,
                parseInt(root.dataset.yearTo, 10) || yearMax
            );

            return {
                reset: function () {
                    // Snap to full extent; the reset handler re-submits once.
                    set(yearMin, yearMax);
                },
            };
        })();

        // ── Live filter: htmx fetches; we intercept the swap ──────────
        var form = document.getElementById('topic-filter-form');
        var resetBtn = document.getElementById('topic-filter-reset');
        var container = document.getElementById('topic-cloud-results');
        if (!form || !container) return;

        // Filter triggers. The form's `hx-trigger` is just "submit" (no event
        // filters), because htmx compiles filter expressions like
        // `change[...]` with the Function constructor, which the strict host
        // CSP blocks (no 'unsafe-eval'). We reproduce the two filters here in
        // plain JS and drive htmx via form.requestSubmit(), which fires the
        // native submit event htmx listens for.
        //   change[!event.target.closest('#tf-actor-popup')] — any field change
        //   except those from inside the actor popup's own search box.
        form.addEventListener('change', function (ev) {
            if (ev.target.closest('#tf-actor-popup')) return;
            form.requestSubmit();
        });
        //   input[event.target.id==='tf-q'] changed delay:350ms — debounced,
        //   and only when the search text actually changed.
        var qInput = document.getElementById('tf-q');
        if (qInput) {
            var qTimer = null;
            var lastQ = qInput.value;
            qInput.addEventListener('input', function () {
                if (qInput.value === lastQ) return;
                lastQ = qInput.value;
                clearTimeout(qTimer);
                qTimer = setTimeout(function () { form.requestSubmit(); }, 350);
            });
        }

        function updateResetVisibility() {
            if (!resetBtn) return;
            var hasValue = false;
            new FormData(form).forEach(function (v) {
                if ((typeof v === 'string' ? v.trim() : v)) hasValue = true;
            });
            resetBtn.hidden = !hasValue;
        }

        // Sub-regions of #topic-cloud-results that get swapped wholesale
        // (in document order). The figure / SVG is handled specially —
        // we keep the SVG element and diff its circles so dots can fade
        // in/out instead of the whole block snapping.
        var TOP_LEVEL_SELECTORS = [
            'div.topic-cloud-outline',
            'div.topic-cloud-table',
            'div.alert',
        ];

        // The evidence count lives full-width above both columns (so the
        // canvas and actor panel align at the top), not inside the cloud
        // column, so swap it from its own host like the actor panel.
        function updateCountHost(fresh) {
            var host = document.getElementById('topic-count-host');
            if (!host) return;
            var existing = host.querySelector('.topic-cloud-count');
            var incoming = fresh ? fresh.querySelector('.topic-cloud-count') : null;
            if (existing && incoming) existing.replaceWith(incoming);
            else if (existing && !incoming) existing.remove();
            else if (!existing && incoming) host.appendChild(incoming);
        }

        // The theme bar narrows on every filter change too (counts and
        // the active chip), so swap it wholesale from the partial response just
        // like the actor panel.
        function updateGroupsHost(fresh) {
            var host = document.getElementById('topic-groups-host');
            if (!host) return;
            var existing = host.querySelector('.topic-groups');
            var incoming = fresh ? fresh.querySelector('.topic-groups') : null;
            if (existing && incoming) existing.replaceWith(incoming);
            else if (existing && !incoming) existing.remove();
            else if (!existing && incoming) host.appendChild(incoming);
        }

        // The main-topic tree narrows on every filter change too (the counts and
        // the active node), so swap it wholesale from the partial response just
        // like the group bar.
        function updateChaptersHost(fresh) {
            var host = document.getElementById('topic-chapters-host');
            if (!host) return;
            var existing = host.querySelector('.topic-chapters');
            var incoming = fresh ? fresh.querySelector('.topic-chapters') : null;
            if (existing && incoming) existing.replaceWith(incoming);
            else if (existing && !incoming) existing.remove();
            else if (!existing && incoming) host.appendChild(incoming);
        }

        // The "Actors in view" panel is recomputed over the filtered set on
        // every change, so swap it wholesale from the partial response like the
        // theme bar.
        function updateActorsHost(fresh) {
            var host = document.getElementById('topic-actors-host');
            if (!host) return;
            var existing = host.querySelector('.topic-actors');
            var incoming = fresh ? fresh.querySelector('.topic-actors') : null;
            if (existing && incoming) existing.replaceWith(incoming);
            else if (existing && !incoming) existing.remove();
            else if (!existing && incoming) host.appendChild(incoming);
        }

        // Mark the active actor's dots in the cloud and toggle the dimming
        // class on the SVG. Re-run after merges since freshly inserted dots
        // arrive without the class.
        function applyActorHighlight() {
            if (!zoomState) return;
            var svg = zoomState.svg;
            var circles = svg.querySelectorAll('circle[data-href]');
            if (!activeActorId) {
                svg.classList.remove('has-actor-highlight');
                Array.prototype.forEach.call(circles, function (c) {
                    c.classList.remove('is-actor-hit');
                });
                return;
            }
            svg.classList.add('has-actor-highlight');
            Array.prototype.forEach.call(circles, function (c) {
                // data-actor is a space-separated list of originator ids.
                var ids = (c.getAttribute('data-actor') || '').split(' ');
                var hit = ids.indexOf(activeActorId) !== -1;
                c.classList.toggle('is-actor-hit', hit);
            });
        }

        // Reflect activeActorId in the panel's pressed/active row styling.
        function updateActorRowStates() {
            var host = document.getElementById('topic-actors-host');
            if (!host) return;
            Array.prototype.forEach.call(
                host.querySelectorAll('.topic-actor-row'),
                function (row) {
                    var on = row.getAttribute('data-actor-id') === activeActorId;
                    row.classList.toggle('is-active', on);
                    row.setAttribute('aria-pressed', on ? 'true' : 'false');
                }
            );
        }

        // After a swap the panel is rebuilt: if the highlighted actor dropped
        // out of the filtered set, clear the highlight; otherwise re-assert it
        // on the new rows + merged dots.
        function syncActorHighlight() {
            var host = document.getElementById('topic-actors-host');
            if (activeActorId && host
                && !host.querySelector(
                    '.topic-actor-row[data-actor-id="' + activeActorId + '"]'
                )) {
                activeActorId = null;
            }
            updateActorRowStates();
            applyActorHighlight();
        }

        function mergeCircles(oldSvg, newSvg) {
            // Index existing circles by their post URL — stable per post,
            // unaffected by filter changes (only presence changes).
            var oldByHref = Object.create(null);
            Array.prototype.forEach.call(
                oldSvg.querySelectorAll('circle[data-href]'),
                function (c) { oldByHref[c.getAttribute('data-href')] = c; }
            );
            var newByHref = Object.create(null);
            Array.prototype.forEach.call(
                newSvg.querySelectorAll('circle[data-href]'),
                function (c) { newByHref[c.getAttribute('data-href')] = c; }
            );

            // Drop circles no longer in the filtered set: fade to 0 via
            // the CSS transition, then remove from the DOM.
            Object.keys(oldByHref).forEach(function (href) {
                if (newByHref[href]) return;
                var c = oldByHref[href];
                if (c.dataset.removing === '1') return;
                c.dataset.removing = '1';
                c.setAttribute('fill-opacity', '0');
                c._removalTimer = setTimeout(function () {
                    unregisterCircle(c);
                    if (c.parentNode) c.parentNode.removeChild(c);
                }, 220);
            });

            // Insert newly-matching circles at opacity 0, then bump to
            // the target opacity in the next frame so the transition
            // fires (a same-frame set+set collapses to the final value).
            Object.keys(newByHref).forEach(function (href) {
                var existing = oldByHref[href];
                if (existing) {
                    // A kept dot keeps its DOM node (so its position is stable),
                    // but its colour can change between requests — most visibly
                    // the theme lens, which re-tints every visible dot to the
                    // selected theme. Copy the incoming fill / dominant-theme so
                    // the recolour actually takes (fill has no CSS transition, so
                    // it switches instantly).
                    existing.setAttribute('fill', newByHref[href].getAttribute('fill'));
                    var dataTheme = newByHref[href].getAttribute('data-theme');
                    if (dataTheme !== null) {
                        existing.setAttribute('data-theme', dataTheme);
                    }
                    // A previously-fading-out circle came back before its
                    // removal timer fired — cancel the removal and fade
                    // it back up to its target opacity.
                    if (existing.dataset.removing === '1') {
                        if (existing._removalTimer) {
                            clearTimeout(existing._removalTimer);
                            existing._removalTimer = null;
                        }
                        existing.dataset.removing = '';
                        var revived = newByHref[href].getAttribute('fill-opacity') || '1';
                        existing.setAttribute('fill-opacity', revived);
                    }
                    return;
                }
                var c = newByHref[href];
                var target = c.getAttribute('fill-opacity') || '1';
                c.setAttribute('fill-opacity', '0');
                oldSvg.appendChild(c);
                registerCircle(c);
                // Force a reflow so the 0 → target change is observed.
                void c.getBoundingClientRect();
                c.setAttribute('fill-opacity', target);
            });
        }

        function applyUpdate(fresh) {
            var oldFigure = container.querySelector('.topic-cloud-svg');
            var newFigure = fresh.querySelector('.topic-cloud-svg');

            // Swap each non-figure top-level section in place; missing
            // sections (e.g. legend disappears when filters yield zero
            // results) are removed, new ones are appended.
            TOP_LEVEL_SELECTORS.forEach(function (sel) {
                var oldEl = container.querySelector(':scope > ' + sel);
                var newEl = fresh.querySelector(':scope > ' + sel);
                if (oldEl && newEl) {
                    oldEl.replaceWith(newEl);
                } else if (oldEl && !newEl) {
                    oldEl.remove();
                } else if (!oldEl && newEl) {
                    container.appendChild(newEl);
                }
            });

            if (oldFigure && newFigure) {
                // Same shape on both sides — diff the circles.
                mergeCircles(
                    oldFigure.querySelector('svg'),
                    newFigure.querySelector('svg')
                );
                // Re-settle the surviving + newly-added dots so the layout
                // stays non-overlapping, then frame whichever subset survived.
                relaxLayout();
                fitToCircles(true);
            } else if (oldFigure && !newFigure) {
                // Empty result — fade the figure out and drop it.
                zoomState = null;
                oldFigure.style.transition = 'opacity 200ms ease';
                oldFigure.style.opacity = '0';
                setTimeout(function () { oldFigure.remove(); }, 220);
            } else if (!oldFigure && newFigure) {
                // Filters relaxed back to a non-empty result — insert
                // the figure above the outline and fade it in. New
                // figure means new event listeners too.
                newFigure.style.opacity = '0';
                var outline = container.querySelector(':scope > div.topic-cloud-outline')
                    || container.querySelector(':scope > div.alert');
                if (outline) container.insertBefore(newFigure, outline);
                else container.appendChild(newFigure);
                bindCloud();
                requestAnimationFrame(function () {
                    newFigure.style.transition = 'opacity 200ms ease';
                    newFigure.style.opacity = '1';
                });
            }
        }

        // htmx drives the request (trigger debounce, cancel, dispatch). We
        // intercept the swap so the SVG figure isn't replaced wholesale —
        // mergeCircles diffs the circles in place so dots fade in/out and
        // the zoom state + relaxed layout survive filter changes.
        document.body.addEventListener('htmx:beforeSwap', function (evt) {
            if (evt.detail.target !== container) return;
            if (!evt.detail.xhr || evt.detail.xhr.status !== 200) return;
            var tmp = document.createElement('div');
            tmp.innerHTML = evt.detail.serverResponse;
            var fresh = tmp.querySelector('#topic-cloud-results');
            if (!fresh) return;
            applyUpdate(fresh);
            // The count line, theme bar, main-topic tree and actor panel live
            // outside #topic-cloud-results, so swap them explicitly from the
            // partial response, then re-assert the actor highlight over the
            // new dots.
            updateCountHost(tmp);
            updateGroupsHost(tmp);
            updateChaptersHost(tmp);
            updateActorsHost(tmp);
            syncActorHighlight();
            evt.detail.shouldSwap = false;
        });

        // We cancelled htmx's swap, so URL sync is on us. Mirror the user
        // path (no extra params — request.htmx handles partial detection).
        document.body.addEventListener('htmx:afterRequest', function (evt) {
            if (evt.detail.target !== container) return;
            container.removeAttribute('aria-busy');
            if (!evt.detail.successful) return;
            var requested = evt.detail.pathInfo && evt.detail.pathInfo.finalRequestPath;
            if (!requested) return;
            var qsAt = requested.indexOf('?');
            var qs = qsAt >= 0 ? requested.slice(qsAt) : '';
            var pageUrl = form.getAttribute('data-reset-url') || window.location.pathname;
            window.history.replaceState(null, '', pageUrl + qs);
        });

        document.body.addEventListener('htmx:beforeRequest', function (evt) {
            if (evt.detail.target !== container) return;
            container.setAttribute('aria-busy', 'true');
        });

        // Theme bar → the hidden #tf-themes input (the single selected theme).
        // Single-select: clicking a chip selects only that theme, narrowing the
        // cloud to its evidence (so every visible dot takes the theme's colour);
        // clicking the active chip again clears it. The theme selection is
        // independent of the main-topic tree — they stack. Delegated on the stable
        // host because updateGroupsHost replaces the bar wholesale.
        var groupsBox = document.getElementById('tf-themes');
        var groupsHost = document.getElementById('topic-groups-host');
        function selectedGroups() {
            if (!groupsBox) return [];
            return Array.prototype.map.call(
                groupsBox.querySelectorAll('input[name="theme"]'),
                function (i) { return i.value; }
            );
        }
        function setGroups(values) {
            if (!groupsBox) return;
            groupsBox.innerHTML = '';
            values.forEach(function (v) {
                var inp = document.createElement('input');
                inp.type = 'hidden';
                inp.name = 'theme';
                inp.value = v;
                groupsBox.appendChild(inp);
            });
            form.requestSubmit();
        }
        if (groupsHost && groupsBox) {
            groupsHost.addEventListener('click', function (ev) {
                var chip = ev.target.closest('.topic-group-chip');
                if (!chip) return;
                var gid = chip.getAttribute('data-theme') || '';
                if (!gid) return;
                // Single-select: re-clicking the active theme clears it, any
                // other click replaces the selection with just that theme.
                var current = selectedGroups();
                var active = current.length === 1 && current[0] === gid;
                setGroups(active ? [] : [gid]);
            });
        }

        // Main-topic tree → the hidden #tf-chapters input (the single selected
        // chapter). Single-select drill-down: clicking a node narrows the cloud
        // to evidence under that chapter's subtree; clicking the active node
        // again clears it. Independent of the theme bar — the two stack.
        // Delegated on the stable host because
        // updateChaptersHost replaces the tree wholesale.
        var chaptersBox = document.getElementById('tf-chapters');
        var chaptersHost = document.getElementById('topic-chapters-host');
        function selectedChapters() {
            if (!chaptersBox) return [];
            return Array.prototype.map.call(
                chaptersBox.querySelectorAll('input[name="chapter"]'),
                function (i) { return i.value; }
            );
        }
        function setChapters(values) {
            if (!chaptersBox) return;
            chaptersBox.innerHTML = '';
            values.forEach(function (v) {
                var inp = document.createElement('input');
                inp.type = 'hidden';
                inp.name = 'chapter';
                inp.value = v;
                chaptersBox.appendChild(inp);
            });
            form.requestSubmit();
        }

        // ── Collapsible tree (client-side) ──────────────────────────────
        // The tree is a flat list of rows linked by data-node-id / data-parent-id
        // and starts collapsed (the server unhides only the path to a selected
        // node). The chevron toggles a node's direct children in/out; collapsing
        // also hides every descendant so a reopened branch starts tidy. This is
        // purely DOM state — no request — so it stays snappy and independent of
        // the filter selection.
        function chapterRowEls() {
            return chaptersHost
                ? chaptersHost.querySelectorAll('.topic-chapter-row')
                : [];
        }
        function directChildRows(id) {
            var out = [];
            Array.prototype.forEach.call(chapterRowEls(), function (row) {
                if (row.getAttribute('data-parent-id') === id) out.push(row);
            });
            return out;
        }
        function setToggleExpanded(toggle, expanded) {
            toggle.classList.toggle('is-expanded', expanded);
            toggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        }
        function collapseDescendants(row) {
            directChildRows(row.getAttribute('data-node-id')).forEach(function (child) {
                child.classList.add('is-hidden');
                var t = child.querySelector('.topic-chapter-toggle');
                if (t && t.classList.contains('is-expanded')) {
                    setToggleExpanded(t, false);
                }
                collapseDescendants(child);
            });
        }
        function toggleChapterNode(toggle) {
            var row = toggle.closest('.topic-chapter-row');
            if (!row) return;
            if (toggle.classList.contains('is-expanded')) {
                setToggleExpanded(toggle, false);
                collapseDescendants(row);
            } else {
                setToggleExpanded(toggle, true);
                directChildRows(row.getAttribute('data-node-id')).forEach(
                    function (child) { child.classList.remove('is-hidden'); }
                );
            }
        }

        if (chaptersHost && chaptersBox) {
            chaptersHost.addEventListener('click', function (ev) {
                // Chevron → expand/collapse (no filter change). The leaf spacer
                // shares the class but carries the --leaf modifier, so skip it.
                var toggle = ev.target.closest('.topic-chapter-toggle');
                if (toggle && !toggle.classList.contains('topic-chapter-toggle--leaf')) {
                    toggleChapterNode(toggle);
                    return;
                }
                var node = ev.target.closest('.topic-chapter-node');
                if (!node) return;
                var cid = node.getAttribute('data-chapter') || '';
                if (!cid) return;
                // Single-select: re-clicking the active node clears it, any
                // other click replaces the selection with just that chapter.
                var current = selectedChapters();
                var active = current.length === 1 && current[0] === cid;
                setChapters(active ? [] : [cid]);
            });
        }

        // "Actors in view" panel → in-cloud highlight (no filtering). Clicking a
        // row marks that actor's dots and dims the rest; clicking the active row
        // again clears it. Delegated on the stable host because updateActorsHost
        // replaces the panel wholesale on each filter change.
        var actorsHost = document.getElementById('topic-actors-host');
        if (actorsHost) {
            actorsHost.addEventListener('click', function (ev) {
                var row = ev.target.closest('.topic-actor-row');
                if (!row) return;
                var id = row.getAttribute('data-actor-id') || '';
                activeActorId = (activeActorId === id) ? null : id;
                updateActorRowStates();
                applyActorHighlight();
            });
        }

        // Toggle reset visibility live as the user edits fields.
        form.addEventListener('change', updateResetVisibility);
        form.addEventListener('input', updateResetVisibility);

        // Reset link: clear the form client-side, then re-submit so htmx
        // sends a request with an empty filter set.
        if (resetBtn) {
            resetBtn.addEventListener('click', function (ev) {
                ev.preventDefault();
                form.reset();
                form.querySelectorAll('select').forEach(function (s) { s.value = ''; });
                form.querySelectorAll('input').forEach(function (i) {
                    if (i.type === 'search' || i.type === 'date' || i.type === 'text') i.value = '';
                });
                // Clear the theme + chapter selections inline (no dispatch — we
                // re-submit once below). form.reset() leaves hidden inputs as-is.
                if (groupsBox) groupsBox.innerHTML = '';
                if (chaptersBox) chaptersBox.innerHTML = '';
                if (actorCombobox) actorCombobox.reset();
                if (yearSlider) yearSlider.reset();
                updateResetVisibility();
                form.requestSubmit();
            });
        }
    })();
