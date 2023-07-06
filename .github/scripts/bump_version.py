import sys

part = sys.argv[1]

with open("VERSION") as file:
    version = file.read().strip().split(".")

if part == "major":
    version[0] = str(int(version[0]) + 1)
    version[1] = "0"
    version[2] = "0"
elif part == "minor":
    version[1] = str(int(version[1]) + 1)
    version[2] = "0"
elif part == "patch":
    version[2] = str(int(version[2]) + 1)
else:
    raise ValueError(f"Invalid part: {part}")

with open("VERSION", "w") as file:
    file.write(".".join(version))
    file.write("\n")
