PY_FILES := $(shell							\
	find . -iname '*.py'						\
	| sort -V 							\
	| tr -s '\n' ' '						\
)

.PHONY: black-inplace black-view clean init mypy patches plots upgrade

# Lets not argue about code style :D
# https://github.com/psf/black#the-uncompromising-code-formatter
black-inplace:
	black --line-length 79 --target-version py312 $(PY_FILES)

black-view:
	black --color --diff --line-length 79 --target-version py312	\
		$(PY_FILES)

# Remove emacs backup files and python compiled bytecode
clean:
	clear
	find . -type f -iname "*~"          -exec rm   -fv {} \;
	find . -type f -iname "*.pyc"       -exec rm   -fv {} \;
	find . -type d -iname "__pycache__" -exec rmdir -v {} \;

# Install required packages with pip
init:
	pip install -r requirements.txt

patches:
	git format-patch -o ~/RAMDISK/ lazzaros

# Upgrade all outdated packages with pip
# ( many thanks to https://stackoverflow.com/a/3452888 )
upgrade:
	pip 	--disable-pip-version-check list			\
		--outdated						\
		--format=json						\
	| python -c "import json, sys; print('\n'.join([x['name'] for x in json.load(sys.stdin)]))" \
	| xargs -n1 pip install -U
