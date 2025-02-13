import argparse
import sys
from os import getcwd

sys.path.insert(0, getcwd()[:-4])  # "~/MyOffice/ProjectEdith/code/sathwick/"

from src.face_detection.FaceDetectionFactory import FaceDetectionFactory
from test.ArgumentSetter import ArgumentSetter

factory = FaceDetectionFactory()
argumentSetter = ArgumentSetter()

# argumentSetter.set_args_for_hog_svn()
# args = argumentSetter.get_args()
# faceDetector = factory.get_face_detector(args, "hog_svn")
# if faceDetector.detect_face():
#     faceDetector.show_detections()

# argumentSetter.set_args_for_dlib_cnn()
# args = argumentSetter.get_args()
# faceDetector = factory.get_face_detector(args, "cnn")
# if faceDetector.detect_face():
#     faceDetector.show_detections()

argumentSetter.set_args_for_ssd_dnn_pretrained()
args = argumentSetter.get_args()
faceDetector = factory.get_face_detector(args, "ssd_dnn_pretrained")

faceDetector.face_detection_realtime()
