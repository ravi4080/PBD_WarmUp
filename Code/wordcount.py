import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('warmupproject') \
    .getOrCreate()
source = spark.sparkContext
text_data = source.textFile("D:\Study\wordcount\ytimes.txt")
filled_lines = text_data.filter(lambda x: len(x) > 0)
# removing urls form the text data
remove_url = filled_lines.filter(lambda x: not x.startswith("URL"))
sum = remove_url.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False)
from wordcloud import WordCloud
import matplotlib.pyplot as plt
word_count = [list(i) for i in sum.collect()]
for x in word_count:
    x[1] = x[1].replace(".", "").replace('”', "").replace(',', "").replace("“", "")
# excluding stopwords from text data
ignore_words = ["A","Mr","And","Also","Make","If","His","Their","It","Two","Be","More","Was","My","Her","Said","When","Last","Including","Who","Will","Or","Present","Is","Ms","Would","Just","Can","Of","He","How","Being","Only","Our","Now","Were","Have","Had","Them","Because","She","Did","One","Did","Get","That","Some","Has","Others","Been","Are","Could","Much","No","They","Most","Not","There","We","The","What","Even","The","Its","You","New","Those","All","Made","Where","Even","Do","Aboard","About","Above","Across","After","a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and", "any", "are","aren","aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but","by", "can","couldn", "couldn't", "d", "did", "didn", "didn't", "do", "does", "doesn", "doesn't", "doing", "don","don't",
                "down", "during", "each", "few", "for", "from", "further", "had", "hadn", "Many","many","Mr","Mr.","time","Time","hadn't", "has", "hasn","hasn't","have", "haven", "haven't", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i","if", "in", "into", "is", "isn", "isn't", "it", "it's", "its", "itself", "just", "ll", "m", "ma", "me","mightn","mightn't", "more", "most", "mustn", "mustn't", "my", "myself", "needn", "needn't", "no", "nor", "not","now","o", "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own","re","s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn", "shouldn't", "so","some",
                "such", "t", "than", "that", "that'll", "the", "their", "theirs", "them", "themselves", "then", "there","these","they", "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn","wasn't","we", "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom", "why","will", "with","won", "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your","yours","yourself", "yourselves", "could", "he'd", "he'll", "he's", "here's", "how's", "i'd", "i'll", "i'm","i've","let's", "ought", "she'd", "she'll", "that's", "there's", "they'd", "they'll", "they're", "they've","we'd","we'll", "we're", "we've", "what's", "when's", "where's", "who's", "why's", "would", "able", "abst","accordance","Against","Along","Amid","Among","Anti","Around","As","At","Before","Behind","Below","Beneath","Beside","Besides","Between","Beyond","But","By","Concerning","Considering","Despite","Down","During","Except","Excepting","Excluding","Following","For","From","In","Inside","Into","Like","Minus","Near""Of","Off","On","Onto","Opposite","Outside","Over","Past","Per","Plus","Regarding","Round","Save","Since","Than","Through","To","Toward","Towards","Under","Underneath","Unlike","Until","Up","Upon","Versus","Via","With","Within","Without","mr","Mr.","a","Mr","and","also","make","if","his","their","it","two","be","more","was","my","her","said","when","last","including","who","will","or","present","is","ms","would","just","can","of","he","how","being","only","our","now","were","have","had","them","because","she","did","one","did","get","that","some","has","others","been","are","could","much","no","they","most","not","there","we","the","what","even","The","its","you","new","those","all","made","where","even","do","aboard","about","above","across","after","against","along","amid","among","anti","around","as","at","before","behind","below","beneath","beside","besides","between","beyond","but","by","concerning","considering","despite","down","during","except","excepting","excluding","following","for","from","in","inside","into","like","minus","near""of","off","on","onto","opposite","outside","over","past","per","plus","regarding","round","save","since","than","through","to","toward","towards","under","underneath","unlike","until","up","upon","versus","via","with","within","without"]
ignore_words.extend(["—", "it’s", "don’t", "i’m"])
extra_words = [y for y in word_count if y[1].lower() not in ignore_words]
# top 100 in dictionary
cloud_dictionary = {extra_words[z][1]: extra_words[z][0] for z in range(len(extra_words)) if z < 100}
# plotting in wordcloud
wordcloud = WordCloud()
wordcloud.generate_from_frequencies(frequencies=cloud_dictionary)
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
# Task 2
most_viewed_5 = {}
for p in filled_lines.collect():
      if p.startswith("URL"):
            classification = p.split('/')[6]
            if classification in most_viewed_5:
                  most_viewed_5[classification] += 1
            else:
                  most_viewed_5[classification] = 1
from collections import Counter
categories = list(dict(Counter(most_viewed_5).most_common(5)).keys())
categories
present_url = ""
address = {}
for i in filled_lines.collect():
      if i.startswith("URL") :
            present_url = i
      else:
            if present_url in address:
                  address[present_url] += i
            else:
                  address[present_url] = i
address
#categories of top 5 in paragraphs
categories_information = ""
for article in address:
      if any(top in article for top in categories):
            categories_information += address[article]
most_viewed_wordcount =categories_information.split(" ")
most_viewed_wordcount = [(i.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for i in most_viewed_wordcount]
top5_ignore_words = [i for i in most_viewed_wordcount if i not in ignore_words]
categories_frequency = {}
for i in top5_ignore_words:

      if i not in categories_frequency:
            categories_frequency[i] = 1
      else:
            categories_frequency[i] += 1
top5cattop100 = dict(Counter(categories_frequency).most_common(100))
top5cattop100
#plotting in wordcloud for top5 category words
wordcloud.generate_from_frequencies(frequencies=top5cattop100)
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
# task 3
categories
#these categories has most number of articles
category_wise = {}
for parts in categories:
    for r in address:
        if parts in r:
            if parts in category_wise:
                category_wise[parts] += address[r]
            else:
                category_wise[parts] = address[r]
category_wise
#top 10 words in sports category
sports_top10 = category_wise['sports']
sports_section_words = sports_top10.split(" ")
sports_section_replace = [(s.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for s in sports_section_words]
sports_section_extrawords = [q for q in sports_section_replace if q not in ignore_words]
Counter(sports_section_extrawords).most_common(10)
#top 10 words in world category
world_top5 = category_wise['world']
world_words = world_top5.split(" ")
world_section_replace = [(v.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for v in world_words]
world_section_extrawords = [w for w in world_section_replace if w not in ignore_words]
Counter(world_section_extrawords).most_common(10)
#top 10 words in us category
us_top10 = category_wise['us']
us_section = us_top10.split(" ")
us_section_replace = [(u.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for u in us_section]
us_words_exsw = [u for u in us_section_replace if u not in ignore_words]
Counter(us_words_exsw).most_common(10)
#top 10 words in business category
business_top10 = category_wise['business']
business_section = business_top10.split(" ")
business_section_replace = [(b.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for b in business_section]
business_section_extrawords = [b for b in business_section_replace if b not in ignore_words]
Counter(business_section_extrawords).most_common(10)
#top 10 words in new york region category
nyregion_top10 = category_wise['nyregion']
nyregion_section = nyregion_top10.split(" ")
nyregion_section_replace = [(n.lower()).replace(".", "").replace('”', "").replace(',', "").replace("“", "") for n in nyregion_section]
nyregion_section_extrawords = [n for n in nyregion_section_replace if n not in ignore_words]
Counter(nyregion_section_extrawords).most_common(10)
text_data.stop()