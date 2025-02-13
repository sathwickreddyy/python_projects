from sys import path
from os import getcwd

path.insert(0, getcwd()[:-4])
from config import Config

from argparse import ArgumentParser


class ArgumentSetter:
    def __init__(self):
        self.argumentParser = ArgumentParser()
        self.mappings = Config().get_dict()

    def get_args(self):
        return self.argumentParser.parse_args()

    def _set_image_arg(self):
        self.argumentParser.add_argument('-i', '--image', required=False,
                                         help='Provide the image Name after dumping the image in '
                                              '.../ProjectEdith/zz_image_dump')

    def set_args_for_hog_svn(self):
        self._set_image_arg()

    def set_args_for_dlib_cnn(self):
        cnn_data_file_name = "mmod_human_face_detector.dat"
        self._set_image_arg()
        self.argumentParser.add_argument('-w', '--weights', help='Path to Weights',
                                         default=self.mappings['DataFilesPath'] + cnn_data_file_name)

    def set_args_for_ssd_dnn_pretrained(self):
        resnet_10_caffe_model_file_name = "res10_300x300_ssd_iter_140000.caffemodel"
        prototxt_file_name = "deploy.prototxt.txt"

        self.argumentParser.add_argument("-p", "--prototxt", default=self.mappings['DataFilesPath']+prototxt_file_name,
                                         help="Caffe 'deploy' prototxt file")
        self.argumentParser.add_argument("-m", "--model",
                                         default=self.mappings['DataFilesPath'] + resnet_10_caffe_model_file_name,
                                         help="Pre-trained caffe model")
        self.argumentParser.add_argument("-t", "--thresold", type=float, default=0.6,
                                         help="Thresold value to filter weak detections")
