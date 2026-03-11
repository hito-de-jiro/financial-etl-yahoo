import sys


def run_ingestion():
    print("[RUN] Ingestion started")
    from ingestion.yahoo_ingestion import main
    main()
    print("[RUN] Ingestion finished\n")


def run_transform():
    print("[RUN] Spark transform started")
    from spark.transform_market_data import main
    main()
    print("[RUN] Spark transform finished\n")


def run_analytics():
    print("[RUN] Analytics started")
    from scripts.run_analytics import main
    main()
    print("[RUN] Analytics finished\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py [ingestion|transform|analytics|all]")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "ingestion":
        run_ingestion()
    elif cmd == "transform":
        run_transform()
    elif cmd == "analytics":
        run_analytics()
    elif cmd == "all":
        run_ingestion()
        run_transform()
        run_analytics()
    else:
        print(f"Unknown command '{cmd}'")
        print("Usage: python run.py [ingestion|transform|analytics|all]")
        sys.exit(1)
