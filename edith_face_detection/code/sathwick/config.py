import os


class Config:
    def __init__(self):
        home_path = os.getcwd()[:-18]
        self.dict = {
            "ProjectHome": home_path,
            "ImageDirectory": home_path + "zz_image_dump/",
            "DataFilesPath": home_path + "zz_data_files_dump/"
        }

    def get_dict(self):
        return self.dict
