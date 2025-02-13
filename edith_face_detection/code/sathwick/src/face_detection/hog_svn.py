import dlib
import cv2
import sys

from os import getcwd, path

sys.path.insert(0, getcwd()[:-18])  # "~/MyOffice/ProjectEdith/code/sathwick/"

from time import time
from src.face_detection.detector import Detector
from config import Config


class HOG_SVN(Detector):

    def __init__(self, args):
        super().__init__(args)
        config = Config()
        dict = config.get_dict()

        # This is based on HOG + SVM classifier
        self.detector = dlib.get_frontal_face_detector()

        self.image = None
        if args.image:
            print("Detecting Faces for the image at path: " + args.image)

            img = path.join(dict["ImageDirectory"], args.image)
            self.image = cv2.imread(img)

    def detect_face(self):
        if self.image is not None:
            t1 = time()

            # Converting the image to gray scale
            gray = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY)

            # The 1 in the second argument indicates that we should upsample the image
            # 1 time.  This will make everything bigger and allow us to detect more
            # faces.

            rectangles = self.detector(gray, 1)

            for (i, rect) in enumerate(rectangles):
                x1, y1, x2, y2, w, h = rect.left(), rect.top(), rect.right() + \
                                       1, rect.bottom() + 1, rect.width(), rect.height()

                self.draw_fancy_box(self.image, (x1, y1), (x2, y2), (127, 255, 255), 2, 10, 20)

                # Drawing simple rectangle around found faces
                # cv2.rectangle(image, (x1, y1), (x1 + w, y1 + h), (0, 255, 0), 2)

                cv2.putText(self.image, "Face #{}".format(i + 1), (x1 - 20, y1 - 20),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.8, (51, 51, 255), 2)

            t2 = time()
            print("\nFace Detection with HOG and SVN Classifier")
            print("Time Taken : " + str(t2 - t1))
            print("No. of faces detected : " + str(len(rectangles)))

            return True

            # self.show_detections(self.image)
        else:
            # raise Exception("Image Not Found")
            print("Image Not Found")
        return False

    def show_detections(self):
        super().show_detections(self.image)
