from .run import main
from .notify import alert

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        alert("Run failed", {"error": str(e)}, severity="critical")
        raise
