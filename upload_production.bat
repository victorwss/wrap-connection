REM python setup.py sdist
python -m twine upload dist/*
pip install -i https://pypi.org/simple/ wrap-connection --force