test:
	ruff check
	pytest

testci:
	coverage run -m pytest
	coverage report

messagesde:
	django-admin makemessages -l de --add-location file --no-obsolete
