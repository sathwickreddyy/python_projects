import cv2


class Detector(object):
    def __init__(self, args=None):
        self.detector = None
        self.args = args

    def get_detector(self):
        return self.detector

    def detect_face(self, *args):
        pass

    def face_detection_realtime(self, *args):
        cap = cv2.VideoCapture(0)

        while True:

            # Getting out image by webcam
            _, image = cap.read()

            # Converting the image to gray scale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

            # Get faces into webcam's image
            rectangles = self.detector(gray, 0)

            face_coordinates = []
            for (i, rect) in enumerate(rectangles):
                x1, y1, x2, y2, w, h = rect.left(), rect.top(), rect.right() + \
                                       1, rect.bottom() + 1, rect.width(), rect.height()

                self.draw_fancy_box(image, (x1, y1), (x2, y2), (127, 255, 255), 2, 10, 20)

                # Drawing simple rectangle around found faces
                # cv2.rectangle(image, (x1, y1), (x1 + w, y1 + h), (0, 255, 0), 2)

                face_coordinates.append((x1, y1, w, h))

                cv2.putText(image, "Face #{}".format(i + 1), (x1 - 20, y1 - 20),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.8, (51, 51, 255), 2)

            # Show the image
            cv2.imshow("Output", image)

            # To capture found faces from camera type s
            if cv2.waitKey(30) & 0xFF == ord('s'):
                self.write_to_disk(image, face_coordinates)

            # type q to quit
            if cv2.waitKey(30) & 0xFF == ord('q'):
                break

        cv2.destroyAllWindows()
        cap.release()

    @staticmethod
    def show_detections(image):
        if image is not None:
            cv2.imshow("Output", image)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
        else:
            raise Exception("Image UnResponsive / Not Found. Please call detect_face() then followed by show detections")

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
