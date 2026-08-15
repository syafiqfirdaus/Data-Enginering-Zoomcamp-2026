.PHONY: local-up local-down producer-install producer-run streaming-install create-table streaming-run-local dashboard-install dashboard-run

local-up:
	docker compose up -d

local-down:
	docker compose down

producer-install:
	cd producer && pip install -r requirements.txt

producer-run:
	cd producer && python main.py

streaming-install:
	cd streaming && pip install -r requirements.txt

create-table:
	cd streaming && python create_table.py

streaming-run-local:
	cd streaming && python local_aggregator.py

dashboard-install:
	cd dashboard && pip install -r requirements.txt

dashboard-run:
	cd dashboard && streamlit run app.py
