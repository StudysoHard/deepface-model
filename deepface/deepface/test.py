import DeepFace
import os

os.environ['CUDA_VISIBLE_DEVICES'] = '/gpu:0'

result = DeepFace.verify(img1_path = "./img1.jpg", img2_path = "./img2.jpg")

print(result)