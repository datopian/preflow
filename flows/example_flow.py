from prefect import flow, task

@task
def say_hello():
    print("👋 Hello from Prefect!")

@flow
def hello_flow(resource_dict,ckan_config ):
    say_hello()

if __name__ == "__main__":
    hello_flow()