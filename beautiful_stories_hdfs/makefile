.PHONY: run-client run

run-client:
	@spark-submit            \
	    --master yarn        \
        --deploy-mode client \
        beautiful_stories.py

run:
	@spark-submit            \
	    --master yarn        \
	    --deploy-mode client \
	    beautiful_stories.py
