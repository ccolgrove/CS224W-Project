import re

f = open("enwiki-latest-categorylinks.sql")
output = open("category_links.csv", 'a')

pattern = re.compile(r"\(([0-9]*),\'(.*?)\',\'(.*?)\',\'(.*?)\',\'(.*?)\',\'(.*?)\',\'(.*?)\'\)")

text = f.read(5000000)


count = 0

while True:
    last_index = 0
    for match in re.finditer(pattern, text):
        g = match.groups()
        output.write("%s,%s,%s,%s\n" % (g[0], g[1], g[3], g[6]))
        count += 1
        if count % 1000000 == 0:
            print count
        start_index, last_index = match.span()

    new_text = f.read(5000000)
    if new_text == '':
        break
    text = text[last_index:] + new_text

f.close()
output.close()
