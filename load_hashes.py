import pickle

# Format is as such:
## file_hashes = {
##     "./MAME/Multimedia/soundtrack/acedrvrw/acedrvrw.txt": {
##         'md5': '216a2452d36182deff9b47402fcac151',
##         'sha1': 'e8e14a97b5c024f9a569cf2a35dd287ca988c693',
##         'sha256': 'b36681a1da545d16e5b5cced16163c96be255c0c1a6184d187340751ae95ad1c'
##     }
## }

file_hashes = None
with open("./file_hashes.bin", "rb") as file:
    file_hashes = pickle.load(file)

for file_path in file_hashes:
    print(file_path)
    print(file_hashes[file_path])
    break
