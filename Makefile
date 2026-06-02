.PHONY: install seed clean

install:        ## install Python deps
	pip install -r requirements.txt

seed:           ## generate the catalog into data/seed/
	python -m afrotix.seed

clean:          ## remove generated seed files
	rm -f data/seed/*.json
