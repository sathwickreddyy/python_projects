"""
    @author: sathwick
"""

import sys
from os import getcwd

sys.path.insert(0, getcwd()[:-18])  # "~/MyOffice/ProjectEdith/code/sathwick/"

from src.face_detection.hog_svn import HOG_SVN
from src.face_detection.dlib_cnn import CNN
from src.face_detection.ssd_dnn_pretrained import SSD_DNN_PRETRAINED


class FaceDetectionFactory:

    def __init__(self):
        pass

    @staticmethod
    def get_face_detector(args, detector_type="hog_svn"):
        target_class = detector_type.upper()

        if target_class == "HOG_SVN":
            return HOG_SVN(args)
        elif target_class == "CNN":
            return CNN(args)
        elif target_class == "SSD_DNN_PRETRAINED":
            return SSD_DNN_PRETRAINED(args)

        return None
