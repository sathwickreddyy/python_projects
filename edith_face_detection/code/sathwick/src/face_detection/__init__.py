import cv2


class Detector(object):
    def __init__(self, args=None):
        self.detector = None
        self.args = args

    def get_detector(self):
        return self.detector

    def face_detection(self, *args):
        pass

    def face_detection_realtime(self, *args):
        pass

    @staticmethod
    def show_detections(image):
        cv2.imshow("Output", image)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    @staticmethod
    def write_to_disk(image, face_coordinates):
        '''
        This function will save the cropped image from original photo on disk
        '''
        for (x1, y1, w, h) in face_coordinates:
            cropped_face = image[y1:y1 + h, x1:x1 + w]
            cv2.imwrite(str(y1) + ".jpg", cropped_face)

    def draw_fancy_box(self, img, pt1, pt2, color, thickness, r, d):
        '''
        To draw some fancy box around founded faces in stream
        '''
        x1, y1 = pt1
        x2, y2 = pt2

        # Top left
        cv2.line(img, (x1 + r, y1), (x1 + r + d, y1), color, thickness)
        cv2.line(img, (x1, y1 + r), (x1, y1 + r + d), color, thickness)
        cv2.ellipse(img, (x1 + r, y1 + r), (r, r), 180, 0, 90, color, thickness)

        # Top right
        cv2.line(img, (x2 - r, y1), (x2 - r - d, y1), color, thickness)
        cv2.line(img, (x2, y1 + r), (x2, y1 + r + d), color, thickness)
        cv2.ellipse(img, (x2 - r, y1 + r), (r, r), 270, 0, 90, color, thickness)

        # Bottom left
        cv2.line(img, (x1 + r, y2), (x1 + r + d, y2), color, thickness)
        cv2.line(img, (x1, y2 - r), (x1, y2 - r - d), color, thickness)
        cv2.ellipse(img, (x1 + r, y2 - r), (r, r), 90, 0, 90, color, thickness)

        # Bottom right
        cv2.line(img, (x2 - r, y2), (x2 - r - d, y2), color, thickness)
        cv2.line(img, (x2, y2 - r), (x2, y2 - r - d), color, thickness)
        cv2.ellipse(img, (x2 - r, y2 - r), (r, r), 0, 0, 90, color, thickness)
