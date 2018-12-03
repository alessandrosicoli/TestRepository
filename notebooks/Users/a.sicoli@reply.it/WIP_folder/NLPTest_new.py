# Databricks notebook source
'''
from gensim.test.utils import common_texts, get_tmpfile
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

documents = [TaggedDocument(doc, [i]) for i, doc in enumerate(common_texts)]
model = Doc2Vec(documents, vector_size=5, window=2, min_count=1, workers=4)
#type(documents)

for a in common_texts:
  print a
print "--------------------"
for b in documents:
  print b
  
vector1 = model.infer_vector(["system", "response"])
print vector1
vector1 = model.infer_vector(["computer", "time"])
print vector1
sims = model.docvecs.most_similar([vector1]) #OK!!!
print sims
'''
'''
from gensim.test.utils import common_texts, get_tmpfile
from gensim.models import Word2Vec

path = get_tmpfile("word2vec.model")

model = Word2Vec(common_texts, size=100, window=5, min_count=1, workers=4)
model.save("word2vec.model")

model = Word2Vec.load("word2vec.model")
model.train([["hello", "world"]], total_examples=1, epochs=1)
vector = model.wv['survey']  # numpy vector of a word
#print vector.size
print vector
#print common_texts
'''

# COMMAND ----------

#ret = dbutils.notebook.run("Word2Vec_WIP", 60, {})
#print ret

# COMMAND ----------

from big_data_projects import msc
mco = msc.marc_init('score_matrix')
#jdText = mco.spark_engine.spark.sql('SELECT reqdescription FROM db_hr_dev.mmhr_JobDescription').collect()
#len(jdText)
from gensim.parsing.preprocessing import remove_stopwords
from gensim.parsing.preprocessing import strip_punctuation
from gensim.parsing.preprocessing import preprocess_string
from gensim.parsing.preprocessing import strip_numeric
from gensim.parsing.preprocessing import strip_multiple_whitespaces
#remove_stopwords("Better late than never, but better never late.")

# COMMAND ----------

all_stopwords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'cannot', 'could', 'did', 'do', 'does', 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'me', 'more', 'most', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', 'she', 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'with', 'would', "wouldn't", 'you', 'your', 'yours', 'yourself', 'yourselves', 'a', 'able', 'about', 'above', 'abst', 'accordance', 'according', 'accordingly', 'across', 'act', 'actually', 'added', 'adj', 'affected', 'affecting', 'affects', 'after', 'afterwards', 'again', 'against', 'ah', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'announce', 'another', 'any', 'anybody', 'anyhow', 'anymore', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apparently', 'approximately', 'are', 'aren', 'arent', 'arise', 'around', 'as', 'aside', 'ask', 'asking', 'at', 'auth', 'available', 'away', 'awfully', 'b', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'begin', 'beginning', 'beginnings', 'begins', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'between', 'beyond', 'biol', 'both', 'brief', 'briefly', 'but', 'by', 'c', 'ca', 'came', 'can', 'cannot', "can't", 'cause', 'causes', 'certain', 'certainly', 'co', 'com', 'come', 'comes', 'contain', 'containing', 'contains', 'could', 'couldnt', 'd', 'date', 'did', 'different', 'do', 'does', 'doing', 'done', 'down', 'downwards', 'due', 'during', 'e', 'each', 'ed', 'edu', 'effect', 'eg', 'eight', 'eighty', 'either', 'else', 'elsewhere', 'end', 'ending', 'enough', 'especially', 'et', 'et-al', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'except', 'f', 'far', 'few', 'ff', 'fifth', 'first', 'five', 'fix', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'found', 'four', 'from', 'further', 'furthermore', 'g', 'gave', 'get', 'gets', 'getting', 'give', 'given', 'gives', 'giving', 'go', 'goes', 'gone', 'got', 'gotten', 'h', 'had', 'happens', 'hardly', 'has', 'have', 'having', 'he', 'hed', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'heres', 'hereupon', 'hers', 'herself', 'hes', 'hi', 'hid', 'him', 'himself', 'his', 'hither', 'home', 'how', 'howbeit', 'however', 'hundred', 'i', 'id', 'ie', 'if', 'im', 'immediate', 'immediately', 'importance', 'important', 'in', 'inc', 'indeed', 'index', 'information', 'instead', 'into', 'invention', 'inward', 'is', 'it', 'itd', 'its', 'itself', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'kg', 'km', 'know', 'known', 'knows', 'l', 'largely', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 'likely', 'line', 'little', 'look', 'looking', 'looks', 'ltd', 'm', 'made', 'mainly', 'make', 'makes', 'many', 'may', 'maybe', 'me', 'mean', 'means', 'meantime', 'meanwhile', 'merely', 'mg', 'might', 'million', 'miss', 'ml', 'more', 'moreover', 'most', 'mostly', 'mr', 'mrs', 'much', 'mug', 'must', 'my', 'myself', 'n', 'na', 'name', 'namely', 'nay', 'nd', 'near', 'nearly', 'necessarily', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'ninety', 'no', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'nor', 'normally', 'nos', 'not', 'noted', 'nothing', 'now', 'nowhere', 'o', 'obtain', 'obtained', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'omitted', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'ord', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'owing', 'own', 'p', 'page', 'pages', 'part', 'particular', 'particularly', 'past', 'per', 'perhaps', 'placed', 'please', 'plus', 'poorly', 'possible', 'possibly', 'potentially', 'pp', 'predominantly', 'present', 'previously', 'primarily', 'probably', 'promptly', 'proud', 'provides', 'put', 'q', 'que', 'quickly', 'quite', 'qv', 'r', 'ran', 'rather', 'rd', 're', 'readily', 'really', 'recent', 'recently', 'ref', 'refs', 'regarding', 'regardless', 'regards', 'related', 'relatively', 'research', 'respectively', 'resulted', 'resulting', 'results', 'right', 'run', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'sec', 'section', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sent', 'seven', 'several', 'shall', 'she', 'shed', 'shes', 'should', 'show', 'showed', 'shown', 'showns', 'shows', 'significant', 'significantly', 'similar', 'similarly', 'since', 'six', 'slightly', 'so', 'some', 'somebody', 'somehow', 'someone', 'somethan', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specifically', 'specified', 'specify', 'specifying', 'still', 'stop', 'strongly', 'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure', 't', 'take', 'taken', 'taking', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'thered', 'therefore', 'therein', "there'll", 'thereof', 'therere', 'theres', 'thereto', 'thereupon', 'these', 'they', 'theyd', 'theyre', 'think', 'this', 'those', 'thou', 'though', 'thoughh', 'thousand', 'throug', 'through', 'throughout', 'thru', 'thus', 'til', 'tip', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'ts', 'twice', 'two', 'u', 'un', 'under', 'unfortunately', 'unless', 'unlike', 'unlikely', 'until', 'unto', 'up', 'upon', 'ups', 'us', 'use', 'used', 'useful', 'usefully', 'usefulness', 'uses', 'using', 'usually', 'v', 'value', 'various', "'ve", 'very', 'via', 'viz', 'vol', 'vols', 'vs', 'w', 'want', 'wants', 'was', 'wasnt', 'way', 'we', 'wed', 'welcome', "we'll", 'went', 'were', 'werent', 'what', 'whatever', 'whats', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'wheres', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whim', 'whither', 'who', 'whod', 'whoever', 'whole', 'whom', 'whomever', 'whos', 'whose', 'why', 'widely', 'willing', 'wish', 'with', 'within', 'without', 'wont', 'words', 'world', 'would', 'wouldnt', 'www', 'x', 'y', 'yes', 'yet', 'you', 'youd', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'z', 'zero', 'magneti', 'marelli', 'corbetta', 'will', 'amp', '?', '(', ')', '!', '|', '<', '>', '&', 'www', '-', '.', '_', ',', '+', '"', "'", '*', '[', ']', '#', '%', "'s", ':', "'s", 'birth', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', '..', '...', '//', '/', 'align=""""left', 'align=""""right', 'curriculum', 'vitaepersonal', 'personal', 'skill', 'dated', 'mail', 'birth', 'date', 'nationality', 'work', 'experience', 'extracurricular', 'informationname', 'surname', 'name', 'orientationadditional', 'informationpersonal', 'mm', 'i.e.', 'align', 'e.g.', "", " ", "et", "al", "'s", "good", "year", 'insight', 'years','daily', 'appropriate', 'degree', 'year year', 'english', 'lt', 'venaria', 'i', 'e', 'corbetta', 'any', 'all', 'tolmezzo', 'etc', 'we','inside', 'he', 'she','heshe','shehe', 'she/he', 'he/she', 'brief', 'description', 'responsabilities', 'minimum', 'requirements', 'fresh']

# COMMAND ----------

originalText_Old001 = "We are looking for a HW Electronic Designer inside the R&D; Function for the Automotive Lighting BL who will be assigned the following responsibilities: - Customer specifications analysis and product requirements definition; - Schematic and layout; - Functional/performances debug and product validation definition; - Product industrialization. - EDUCATION: University degree in electronic (3 or 5 years). - LANGUAGES: English = fluent; - EXPERIENCES: 3 years experienced in HW design a- SPECIFIC KNWOLEDGE: Design tool: Cadence Allegro schematic and PCB Editor It is appreciated knowledge on automotive LED driver design, knowledge of LabVIEW and NI virtual instrumentation, knowledge of CANape SW"

originalText_Old002 = "Magneti Marelli is looking for an ICT Security Senior Specialist to join the Security team in Corbetta Headquarters. He/She will directly report to MM CISO and will be responsible for: \\- Support the company management in the definition and implementation of forensic strategies \\- Conduct data breach, compliance, antitrust and security incident investigations \\- Identify systems/networks compromised by external attacks \\- Coordinate communication with other Group incident response team in order to support security incident management \\- Compile and collect technical evidences for investigations or legal cases and support internal or external attorneys with technical reports and advices \\- Monitor threat intelligence sources and give timely and proper advice on cybersecurity threats landscape \\- Give technical advice to the relevant company references on privacy, safety and cyber-attacks ICT connected cases \\- Support CISO in Identifying and managing cybersecurity strategy and projects for the enterprise \\- Ensure projects and plans are aligned to the industry trends and the latest threats and vulnerabilities Requirements \\- Technical Degree \\- At least 6 years' experience \\- Experience in incident response processes, threat intelligence, SOC/CSIRT/CERT organization \\- Knowledge of cryptography, key management, crypto devices management \\- Good knowledge of written and spoken English language \\- CISSP, GSEC, CompTIA Security +, CRISC and othe Security Certifications will be considered a plus"

originalText_Old003 = "**_1.0 BRIEF DESCRIPTION:_** * Responsible for the sale of Automotive Lighting s product Portfolio to automotive manufacturers and Tier 2 suppliers. * Facilitates interface between customer and the company s group disciplines to ensure customer expectations are met and company standards are maintained. * This position establishes relationships with customer group disciplines such as purchasing and engineering to obtain potential business opportunities. --- 1. **_MAIN RESPONSIBILITIES:_** * Maintains regular contact with customer groups as necessary * Participates in establishing product pricing and offerings * Assists upper management in the preparation of sales plan including specific goals and potentials as well as action plans and strategies for each account * Prepares special reports as required to support management * Provide technical assistance where needed * Track and ensure timely resolution of all customer commercial issues * Interface with our AL Lighting plants to provide program management support * May provide guidance to/and mentor colleagues * Other duties as assigned **_3.0 AUTHORITY:_** * Act as proxy for Sales Manager NAFTA where appropriate. Spending Authority not to exceed $1000 **_4.0 MINIMUM REQUIREMENTS:_** * **_Education:_** \\- Bachelor s degree in Business or Engineering or related field or equivalent combination of education and experience * **_Experience:_** \\- 3-5 years in the automotive industry \\- Experience with FCA and other OEMs * **_Skills:_** \\- Ability to read and interpret part drawings \\- Ability to interpret and analyze cost data \\- Computer literacy \\- Availability to travel to make customer contacts \\- Ability to prepare and deliver company presentations"

originalText_Old004 = "We are looking for a Process Engineer for Powertrain Changchun Plant. He/She will report to the Process Supervisor and will be in charge of below works: Job Responsibilities: 1.Be responsible for developing related project WI, PFMEA, flow chart, process card, etc. 2.Organize the capability study and analysis, responsible for PV batch assembly and shipment. 3.Make necessary communication with R&D;, purchase, logistic, maintenance, quality department to resolve some problem. 4.Team-leader of new projects process development. 5.Lead process improvements through UTE-Team activity. 6.Respect the rules in order to ensure that the process is in line with the environment and security constraints. 7.Follow WCM steps. 8.Ensure effective process control system, manage manufacturing operation and assure machinery cycle time, operator balance, based on customer's requirement and minimize variable production cost. 9.Define and maintain the product package plan though communicate with the customer. 10.Work timely on other tasks as assigned by the Dept. supervisor."

originalText = "Assistant Application Engineer -2019 Fresh Responsibilities 1. Assist vehicle calibrations, with the target to meet emission limitation, excellent drivability, idle stability and good performance in other working conditions 2. Prepare and manage vehicles and equipment for tests 3. Operate tests both on chassis dyno & road. Participate in summer, winter and altitude road test, and then finish test reports on time. 4. Perform ECU flashing activities. Qualifications: 1. Bachelor or above preferred in Internal Combustion Engine Engineering or engine electrical control related major; 2. Familiar with gasoline or/and diesel engine; 3. Basic PC skills especially for MS-Office software; 4. Good level of English in both spoken and written; 5. Have driving license is a plus; 6. Hard working and can work under extreme environment."

noStopWords = remove_stopwords(originalText)

#noPunctation = strip_punctuation(noStopWords)
from string import punctuation

admitted = '&/'
def strip_punctuationCustom(s):
    return ''.join(c for c in s if c in admitted  or c not in punctuation)

noPunctation = strip_punctuationCustom(noStopWords)

print originalText

noPunctList = noPunctation.split()

noCustomSW = [elem for elem in noPunctList if elem.lower() not in all_stopwords]

print "-------------------------------------------"

noCustomSWString = ' '.join(noCustomSW) 

finalResult = strip_multiple_whitespaces(strip_numeric(noCustomSWString))

print finalResult

finalList = finalResult.split()
finalRDD = mco.spark_engine.spark.sparkContext.parallelize(finalList)
finalRDDIndexed = finalRDD.zipWithIndex()
finalRDDIndexed.collect()

# COMMAND ----------

