# Prefect flow placeholder (guarded imports)
try:
    from prefect import flow, task

    @task
    def hello():
        print("hello from Prefect")

    @flow
    def loyalty_flow():
        hello()

    if __name__ == "__main__":
        loyalty_flow()
except Exception:
    if __name__ == "__main__":
        print("Prefect not installed — run as a stub")
