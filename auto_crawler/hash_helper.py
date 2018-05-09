import hashlib
import os,sys

class HashHelper(object):
    def __init__(self):
        pass

    def calc_result(self, hashlib_obj, filepath):
        if os.path.isfile(filepath):
            with open(filepath,'rb') as f:
                content = f.read()
        else:
            content = filepath
        hashlib_obj.update(content)
        hash = hashlib_obj.hexdigest()
        return hash

    def calc_sha1(self, filepath):
        sha1obj = hashlib.sha1()
        return self.calc_result(sha1obj, filepath)

    def calc_md5(self, filepath):
        sha1obj = hashlib.md5()
        return self.calc_result(sha1obj, filepath)

    def calc_sha256(self, filepath):
        sha1obj = hashlib.sha256()
        return self.calc_result(sha1obj, filepath)


if __name__ == "__main__":
    hash_helper_obj = HashHelper()
    print hash_helper_obj.calc_sha256(sys.argv[1])
    print hash_helper_obj.calc_sha1(sys.argv[1])