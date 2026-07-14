from django.utils.translation import gettext_lazy as _

from cms.plugin_base import CMSPluginBase
from cms.plugin_pool import plugin_pool

from .models import TopicCloudCMSPlugin
from .views import EvidenceTopicCloudView


@plugin_pool.register_plugin
class EvidenceTopicCloudPlugin(CMSPluginBase):
    model = TopicCloudCMSPlugin
    module = _("Evidence Collection")
    name = _("Topic cloud")
    render_template = "froide_evidencecollection/topic_cloud.html"

    def render(self, context, instance, placeholder):
        context = super().render(context, instance, placeholder)
        view = EvidenceTopicCloudView()
        view.setup(context["request"])
        context.update(view.get_page_context())
        return context
