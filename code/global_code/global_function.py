def search_file(file_path):
    import os
    import sys
    sys.dont_write_bytecode = True
    file_name = []
    for parent, surnames, filenames in os.walk(file_path):
        for fn in filenames:
            file_name.append(os.path.join(parent, fn))
    return file_name
