BKP_NAME ?= /dev/null
LLM_ALIVE ?= 6h
LLM_NAME ?= "llama3.1:70b"

PY_FILES := $(shell							\
	find . -iname '*.py' ! -ipath '*/pipelines/*'			\
	| sort -V 							\
	| tr -s '\n' ' '						\
)

PIPELINE_FILES := $(shell						\
	find . -iname '*.py' -ipath '*/pipelines/*'			\
	| sort -V 							\
	| tr -s '\n' ' '						\
)

.PHONY: black-inplace black-view clean edit edit-pipelines env init mypy neo4j-dump neo4j-load ollama-pull ollama-run patches upgrade

# Lets not argue about code style :D
# https://github.com/psf/black#the-uncompromising-code-formatter
black-inplace:
	black --line-length 79 --target-version py312			\
		$(PY_FILES) $(PIPELINE_FILES)

black-view:
	black --color --diff --line-length 79 --target-version py312	\
		 $(PY_FILES) $(PIPELINE_FILES)

# Remove emacs backup files and python compiled bytecode
clean:
	clear
	rm -vf .markdown-preview.html extracted_code.py
	rm -vf $(MY_NEO4J_HOST_BKP_DIR)/$(MY_NEO4J_DB_NAME).dump
	ln -vfs pipelines/formatted/empty.py extracted_code.py
	find . -type f -iname ".DS_Store"   -exec rm   -fv {} \;
	find . -type f -iname "*~"          -exec rm   -fv {} \;
	find . -type f -iname "*.pyc"       -exec rm   -fv {} \;
	find . -type d -iname "__pycache__" -exec rmdir -v {} \;

edit:
	emacs -nw $(PY_FILES)

edit-pipelines:
	emacs -nw $(PIPELINE_FILES)

env:
	@cat docker-compose/.env		\
	| sed 's/$$/"/'				\
	| sed 's/^/export /'			\
	| sed 's/=/="/'				\
	| tr -s '\n' '; '			\
	; echo

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

# https://neo4j.com/docs/operations-manual/current/docker/dump-load
neo4j-dump:
	@if [ "/dev/null" = "$(BKP_NAME)" ]; then			\
		echo -e 'Usage:\n\tBKP_NAME="" make neo4j-dump\n\n';	\
		sleep 5;						\
	fi
	@if   [ -z "$(MY_NEO4J_HOST_BKP_DIR)"  ]			\
	   || [ -z "$(MY_NEO4J_HOST_DATA_DIR)" ]			\
	   || [ -z "$(MY_NEO4J_DB_NAME)"       ]; then			\
		echo 'source <(make env)';				\
		sleep 5;						\
	fi
	docker exec -it neo4j neo4j-admin server stop
	chown -R 7474:7474 $(MY_NEO4J_HOST_BKP_DIR)
	docker run \
		--interactive						\
		--rm							\
		--tty							\
		--volume=$(MY_NEO4J_HOST_BKP_DIR):/backups		\
		--volume=$(MY_NEO4J_HOST_DATA_DIR):/data		\
	       neo4j:community						\
	       neo4j-admin database dump --to-path=/backups		\
				$(MY_NEO4J_DB_NAME)			\
	&& cp -v $(MY_NEO4J_HOST_BKP_DIR)/$(MY_NEO4J_DB_NAME).dump $(BKP_NAME)

neo4j-load:
	@if [ "/dev/null" = "$(BKP_NAME)" ]; then			\
		echo -e 'Usage:\n\tBKP_NAME="" make neo4j-load\n\n';	\
		sleep 5;						\
	fi
	@if   [ -z "$(MY_NEO4J_HOST_BKP_DIR)"  ]			\
	   || [ -z "$(MY_NEO4J_HOST_DATA_DIR)" ]			\
	   || [ -z "$(MY_NEO4J_DB_NAME)"       ]; then			\
		echo 'source <(make env)';				\
		sleep 5;						\
	fi
	cp -v $(BKP_NAME) $(MY_NEO4J_HOST_BKP_DIR)/$(MY_NEO4J_DB_NAME).dump
	chown -R 7474:7474 $(MY_NEO4J_HOST_BKP_DIR)
	docker run							\
		--interactive						\
		--rm							\
		--tty							\
		--volume=$(MY_NEO4J_HOST_BKP_DIR):/backups		\
		--volume=$(MY_NEO4J_HOST_DATA_DIR):/data		\
	       neo4j:community						\
	       neo4j-admin database load				\
				--from-path=/backups			\
				--overwrite-destination=true		\
				$(MY_NEO4J_DB_NAME)
	docker run							\
		--detach						\
		--rm							\
		--tty							\
		--volume=$(MY_NEO4J_HOST_DATA_DIR):/data		\
	       neo4j:community						\
	       neo4j-admin server start

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
	| python -c "import json, sys; print('\n'.join(sorted((x['name'] for x in json.load(sys.stdin)), key=str.lower)))" \
	| xargs -n1 pip install -U
