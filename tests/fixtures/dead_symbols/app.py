from fastapi import FastAPI
from pkg.utils import helper_used

app = FastAPI()


def dead_function():
    """This function is never called — should appear in dead_symbols."""
    return "dead"


@app.get("/health")
def health_check():
    """HTTP route handler — should NOT appear in dead_symbols (it's an entry point)."""
    return {"status": "ok"}


def live_function():
    """This function is called by run() — should NOT appear in dead_symbols."""
    return helper_used()


def run():
    return live_function()
