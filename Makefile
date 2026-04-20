test:
	ruff check
	pytest

testci:
	coverage run -m pytest
	coverage report

messagesde:
	python manage.py extendedmakemessages -l de --add-location file --no-wrap --sort-output --keep-header
