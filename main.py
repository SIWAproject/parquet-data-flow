from src.utils import get_projectid_all, upload_parquets_otus_for_projects, upload_parquets_meta_for_projects, client

def main():
    s3_client = client('s3')
    projects = get_projectid_all()
    upload_parquets_otus_for_projects(projects, s3_client)
    upload_parquets_meta_for_projects(projects, s3_client)

if __name__ == "__main__":
    main()