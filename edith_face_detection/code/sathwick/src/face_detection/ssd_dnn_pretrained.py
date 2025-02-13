"""

    Created on Sun Dec 2 20:54:11 2018

    @author: sathwick

    SSD pretrained caffe model based face detection using it with opencv's dnn module.
    (https://docs.opencv.org/3.4.0/d5/de7/tutorial_dnn_googlenet.html)

    python <__name__>.py -p <prototxt> -m <caffe-model> -t <thresold>

"""

import cv2
import sys
import numpy as np

from os import getcwd

sys.path.insert(0, getcwd()[:-18])  # "~/MyOffice/ProjectEdith/code/sathwick/"

from imutils import video
from src.face_detection.detector import Detector

class SSD_DNN_PRETRAINED(Detector):

    def __init__(self, args):
        super().__init__(args)
        self.thresold = 0.55

        self.detector = cv2.dnn.readNetFromCaffe(args.prototxt, args.model)

        if not (args.prototxt and args.model):
            raise Exception("Prototxt/model Not Found: Provide prototxt and model for this face detection algorithm")

        if args.thresold:
            self.thresold = args.thresold

        print("In SSD_DNN_PRETRAINED CLASS")

    def find_faces(self, img, detections):
        total_faces = 0

        # Draw boxes around found faces
        for i in range(0, detections.shape[2]):
            # Probability of prediction
            prediction_score = detections[0, 0, i, 2]
            if prediction_score < self.thresold:
                continue
            # Finding height and width of frame
            (h, w) = img.shape[:2]
            # compute the (x, y)-coordinates of the bounding box for the
            # object
            box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
            (x1, y1, x2, y2) = box.astype("int")

            total_faces += 1

            prediction_score_str = "{:.2f}%".format(prediction_score * 100)

            label = "Face #{} ({})".format(total_faces, prediction_score_str)

            self.draw_fancy_box(img, (x1, y1), (x2, y2), (127, 255, 255), 2, 10, 20)
            cv2.putText(img, label, (x1 - 20, y1 - 20),
                        cv2.FONT_HERSHEY_TRIPLEX, 0.6, (51, 51, 255), 2)

        cv2.imshow("Face Detection with SSD", img)

    # @Override
    def face_detection_realtime(self):
        # Feed from computer camera with threading
        cap = video.VideoStream(src=0).start()

        while True:

            # Getting out image frame by webcam
            img = cap.read()

            # https://docs.opencv.org/trunk/d6/d0f/group__dnn.html#ga29f34df9376379a603acd8df581ac8d7
            inputBlob = cv2.dnn.blobFromImage(cv2.resize(
                img, (300, 300)), 1, (300, 300), (104, 177, 123))

            self.detector.setInput(inputBlob)
            detections = self.detector.forward()
            self.find_faces(img, detections)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                break

        cv2.destroyAllWindows()
        cap.stop()
