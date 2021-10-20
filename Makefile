run:
	docker build -t bud_task .
	docker run -v $(PWD)/results:/app/results bud_task