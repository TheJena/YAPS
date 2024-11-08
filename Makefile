LLM_ALIVE ?= 6h
LLM_NAME ?= "llama3.1:70b"

PY_FILES := $(shell							\
	find . -iname '*.py'						\
	| sort -V 							\
	| tr -s '\n' ' '						\
)

.PHONY: black-inplace black-view clean init mypy ollama-pull ollama-run patches upgrade

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
	rm -vf .markdown-preview.html extracted_code.py
	ln -vfs pipelines/formatted/empty.py extracted_code.py
	find . -type f -iname ".DS_Store"   -exec rm   -fv {} \;
	find . -type f -iname "*~"          -exec rm   -fv {} \;
	find . -type f -iname "*.pyc"       -exec rm   -fv {} \;
	find . -type d -iname "__pycache__" -exec rmdir -v {} \;

# Install required packages with pip
init:
	pip install -r requirements.txt

# Loop forever and show mypy hints at each modification of .py files
mypy:
	clear
	while sleep 1; do						\
		fswatch -1 --event Updated $(PY_FILES)			\
		&& clear						\
		&& mypy $(PY_FILES)					\
		; echo -e "\n\treveal_type( expr )\t may help you :D"	\
	; done

ollama-pull:
	docker exec --detach --interactive --tty ollama			\
		ollama run $(LLM_NAME) --keepalive $(LLM_ALIVE)

ollama-run:
	docker exec --interactive --tty ollama				\
		ollama run $(LLM_NAME) --keepalive $(LLM_ALIVE)

patches:
	rm -vf ./patches/*.patch
	git format-patch -o ./patches/ lazzaros

# Upgrade all outdated packages with pip
# ( many thanks to https://stackoverflow.com/a/3452888 )
upgrade:
	pip 	--disable-pip-version-check list			\
		--outdated						\
		--format=json						\
	| python -c "import json, sys; print('\n'.join([x['name'] for x in json.load(sys.stdin)]))" \
	| xargs -n1 pip install -U
