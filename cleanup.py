import os
import shutil

# Files to delete
files = {
    "h2a.csv", 
    "h2b.csv",
    "jo.csv"
}


folders = {
    "h2b",
    "h2a",
    "jo",
    "zipcache"
}



def cleanup():
    for file_path in files:
        if os.path.isfile(file_path):
            print(f"Deleting file: {file_path}")
            os.remove(file_path)


    for folder_path in folders:
        if os.path.isdir(folder_path):
            print(f"Deleting folder: {folder_path}")
            shutil.rmtree(folder_path)

    # os.makedirs("zipcache", exist_ok=True)
    # print("Created exmpty folder: zipcache")

    print("Cleanup completed.")



if __name__ == "__main__":
    cleanup()