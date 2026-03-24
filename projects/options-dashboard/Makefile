.PHONY: tf-init tf-plan tf-apply tf-destroy producer-install producer-run dashboard-install dashboard-run

# Terraform commands
tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply -auto-approve

tf-destroy:
	cd terraform && terraform destroy

# Producer commands
producer-install:
	cd producer && pip install -r requirements.txt

producer-run:
	cd producer && python main.py

# Dashboard commands
dashboard-install:
	cd dashboard && pip install -r requirements.txt

dashboard-run:
	cd dashboard && streamlit run app.py
