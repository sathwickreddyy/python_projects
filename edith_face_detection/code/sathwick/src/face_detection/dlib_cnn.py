import sys, cv2

from os import getcwd, path

sys.path.insert(0, getcwd()[:-18])  # "~/MyOffice/ProjectEdith/code/sathwick/"

import dlib
from src.face_detection.detector import Detector
from time import time
from config import Config


class CNN(Detector):

    def __init__(self, args):
        super().__init__(args)
        config = Config()
        dict = config.get_dict()

        if args.weights is None:
            raise Exception("No Weights Detected")

        self.detector = dlib.cnn_face_detection_model_v1(args.weights)

        print("In dlib_cnn Detector Class")

        self.image = None
        if args.image:
            print("Detecting Faces for the image at path: " + args.image)

            img = path.join(dict["ImageDirectory"], args.image)

            self.image = cv2.imread(img)

    def detect_face(self):
        if self.image is None:
            print("Image Not Found")
            return False

        t1 = time()
        gray = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY)
        rectangles = self.detector(gray, 0)

        for (i, rect) in enumerate(rectangles):
            x1, y1, x2, y2, w, h = rect.rect.left(), rect.rect.top(), rect.rect.right() + \
                                   1, rect.rect.bottom() + 1, rect.rect.width(), rect.rect.height()

            self.draw_fancy_box(self.image, (x1, y1), (x2, y2), (127, 255, 255), 2, 10, 20)

            # cv2.rectangle(image, (x1, y1), (x1 + w, y1 + h), (0, 255, 0), 2)

            cv2.putText(self.image, "Face #{}".format(i + 1), (x1 - 20, y1 - 20),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.8, (51, 51, 255), 2)

        t2 = time()
        print("\nFace Detection with Deep CNN (mmod_human_face_detector):")
        print("Time Taken : " + str(t2 - t1))
        print("No. of faces detected : " + str(len(rectangles)))

        return True

    def show_detections(self):
        super().show_detections(self.image)
