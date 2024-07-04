import argparse
from src.flow_pets_full_data import load_pet_table_athena
from src.flow_prod_full_data import load_prod_table_athena
from src.flow_prod_otu_count import load_prod_otucount_table_athena  
from src.flow_pets_otu_count import load_pet_otucount_table_athena

def parse_args():
    parser = argparse.ArgumentParser(description='Run data processing and upload to Athena based on project type and specific data processing.')
    parser.add_argument('--project_id', type=str, required=True, help='Project ID to process data for.')
    parser.add_argument('--type', type=str, choices=['pets', 'prod'], required=True, help='Type of data to process: pets or prod.')
    parser.add_argument('--otu', action='store_true', help='Flag to process OTU data.')

    return parser.parse_args()

def main():
    args = parse_args()

    if args.otu:
        if args.type == 'prod':
            load_prod_otucount_table_athena([args.project_id])
        
        elif args.type == 'pets':
            load_pet_otucount_table_athena([args.project_id])
    else:
        if args.type == 'pets':
            load_pet_table_athena([args.project_id])
        elif args.type == 'prod':
            load_prod_table_athena([args.project_id])

if __name__ == '__main__':
    main()
