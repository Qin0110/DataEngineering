import time
from neo4j_pipeline.neo4jLoader import run_loader
from neo4j_pipeline.neo4jQueries import run_analytic_queries

def main():
    print("================================================================================================")
    print("\t\t\t\tDELIVERY DATA ENGINEERING PIPELINE")
    print("================================================================================================\n")
    
    start_time = time.time()
    
    try:
        print("------------------------------------ Executing Graph Loader ------------------------------------")
        run_loader()
        print("\n-----------------------------------------------------------------------------------------------\n")
        
        print("---------------------------------- Executing Graph Analytics ----------------------------------")
        run_analytic_queries()
        
    except Exception as e:
        print(f"\n[ERROR] The pipeline failed: {e}")
        return

    end_time = time.time()
    total_time = round(end_time - start_time, 2)
    
    print("\n================================================================================================")
    print(f"  PIPELINE SUCCESSFULLY EXECUTED")
    print(f"  Total Execution Time: {total_time} seconds")
    print("================================================================================================")

if __name__ == "__main__":
    main()