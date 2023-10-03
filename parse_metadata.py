

chunks = []

with open("post_insert_metadata.txt", "r") as f:
    for line in f:
        chunks = line.split(" ")
            

        

def parse_chunks(chunks):
    group = []
    for i, c in enumerate(chunks):
        for op in ["{", "[", "("]:
            if op in c:
                group.append(parse_chunks(chunks[i + 1::]))
        else:
            for cl in ["}", "]", ")"]:
                if cl in c:
                    return group
        group.append(c)

parsed = parse_chunks(chunks)

with open("parsed_post_metadata.txt", "w") as f:
    for el in parsed:
        f.write(f"====================\n{el}\n=======================")

tabs = 0
follow = ""
temp_tab = False

with open("quick_parsed.txt", "w") as f:
    for el in chunks:
        if el in ["}", "},", "}]),", "}]"]:
            follow = f"\n"
            tabs  -= 1
        else:
            follow = ""

        f.write(f"{'    ' * tabs}{el}\n{follow}")

        if  el in ["{"]:
            tabs += 1


        if temp_tab:
            tabs -= 1
            temp_tab = False

        if el[-1] == ":":
            tabs += 1
            temp_tab = True
