#!/usr/bin/env python
# coding: utf-8

# In[1]:


# #we have 4471 rows and 27 columns in a dataset each row is a DCA issues License,

# Column Name	Description	Type
# DCA License Number-An identification number issued to businesses/individuals to operate legally for the duration of their license term.
# License Type-Indicates whether the licensee is a business or an individual.
# License Expiration Date	- Expiration date of the License.
# License Status-This indicates whether the license is active or inactive.
# License Creation Date-The date the license record was created.
# Industry-The business category or business activity requiring a DCA-issued license in order to operate legally.
# Business Name-The legal business name as filed with the New York State Secretary of State or County Clerk or, if an individual, the person’s first name and last name.
# Business Name 2-If applicable, the Doing-Business-As (DBA), or trade name.
# Address Building-Building number of the business's premise address.
# Address Street Name-Street name of the business's premise address.
# Secondary Address Street Name	-The cross street of the business's premise address.
# Address City-City where the business is located. If an individual license, the City of the licensee's mailing address.
# Address State-State where the business is located. If an individual license, the State of the licensee's mailing address.
# Address ZIP	- Postcode where the business is located. If an individual license, the postcode of the licensee's mailing address.
# Contact Phone Number-The phone number of the business.
# Address Borough-Borough name where the business is located.
# Borough Code-Borough Code of the Borough where the business is located.
# # Community Board-Community District number where the business is located.
# Council District-City Council District number where the business is located.
# BIN-Building Identification Number where the business is located.
# BBL-Borough Block Lot number where the business is located.
# NTA-Neighborhood Tabulation Area where the business is located.
# Census Tract-Census Tract where the business is located.
# Detail-Provides additional licensing details for certain industries.
# Longitude-The longitude of the business's premise address. Not available for individual licenses.
# Latitude-The latitude of the business's premise address.

# below are the missing values for the above columns 


# DCA License Number                  0
# License Type                        0
# License Expiration Date             1
# License Status                      0
# License Creation Date               0
# Industry                            0
# Business Name                       0
# Business Name 2                  3589
# Address Building                   72
# Address Street Name                 0
# Secondary Address Street Name    4218
# Address City                        0
# Address State                       4
# Address ZIP                         0
# Contact Phone Number              101
# Address Borough                     0
# Borough Code                        0
# Community Board                     4
# Council District                    9
# BIN                               256
# BBL                               256
# NTA                              1043
# Census Tract                     1043
# Detail                           4471
# Longitude                          10
# Latitude                           10
# Location                           10
# dtype: int64

# please write a python code to check for 1.Check for missing values 2. handle missing values 3.check for outliers 4.Normalize or scale the data and encode categorical data 5. visualize the data ?


# In[2]:


#importing data from local machine to ubuntu

get_ipython().system('pip install pymysql')
get_ipython().system('pip install mysql-connector-python')


# In[3]:


import mysql.connector
import pandas as pd


# In[4]:


tobacco_business_data = pd.read_csv('Active_Tobacco_Retail_Dealer_Licenses.csv')


# In[5]:


tobacco_business_data.head(10)


# In[6]:


import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://dap_user:Dap_123456789@87.44.4.77:3306/newyork_businesses")

tablename = "tobacco_dealer_licences"

tobacco_business_data.to_sql(tablename, con=engine, if_exists="append", index=False)


# In[7]:


#export data from ubuntu to local system 

get_ipython().system('pip install mysql-connector-python')


# In[8]:


import mysql.connector


# In[9]:


cnx = mysql.connector.connect(user='dap_user', password='Dap_123456789', host='87.44.4.77', database='newyork_businesses')


# In[10]:


query = "SELECT * FROM tobacco_dealer_licences"
tobacco_business_data = pd.read_sql_query(query, cnx)


# In[11]:


# Check for missing values
print(tobacco_business_data.isnull().sum())


# In[12]:


# Handle missing values
tobacco_business_data['License Expiration Date'].fillna(tobacco_business_data['License Expiration Date'].mode()[0], inplace=True)

#fills missing values in the 'License Expiration Date' column with the mode (most frequent value) of that column.


# In[13]:


#fills missing values in the 'Business Name 2' column with the string 'N/A'.
tobacco_business_data['Business Name 2'].fillna('N/A', inplace=True)


# In[14]:


#line fills missing values in the 'Address Building' column with the mode (most frequent value) of that column.
tobacco_business_data['Address Building'].fillna(tobacco_business_data['Address Building'].mode()[0], inplace=True)

#ignore this


# In[15]:


tobacco_business_data['Secondary Address Street Name'].fillna('N/A', inplace=True)

#fills missing values in the 'Secondary Address Street Name' column with the string 'N/A'.


# In[16]:


tobacco_business_data['Address State'].fillna('NY', inplace=True)

#fills missing values in the 'Address State' column with the string 'NY'.


# In[17]:


tobacco_business_data['Contact Phone Number'].fillna('N/A', inplace=True)

#fills missing values in the 'Contact Phone Number' column with the string 'N/A'.


# In[18]:


tobacco_business_data['Community Board'].fillna(tobacco_business_data['Community Board'].mode()[0], inplace=True)

#fills missing values in the 'Community Board' column with the mode (most frequent value) of that column.


# In[19]:


tobacco_business_data['Council District'].fillna(tobacco_business_data['Council District'].mode()[0], inplace=True)

#fills missing values in the 'Council District' column with the mode (most frequent value) of that column.


# In[20]:


tobacco_business_data['BIN'].fillna(tobacco_business_data['BIN'].mode()[0], inplace=True)

#fills missing values in the 'BIN' column with the mode (most frequent value) of that column.


# In[21]:


tobacco_business_data['BBL'].fillna(tobacco_business_data['BBL'].mode()[0], inplace=True)


#fills missing values in the 'BBL' column with the mode (most frequent value) of that column.


# In[22]:


tobacco_business_data['NTA'].fillna('N/A', inplace=True)

#fills missing values in the 'NTA' column with the string 'N/A'.


# In[23]:


tobacco_business_data['Census Tract'].fillna('N/A', inplace=True)

#fills missing values in the 'Census Tract' column with the string 'N/A'.


# In[24]:


#tobacco_business_data.drop(['Detail'], axis=1, inplace=True)

#This line drops the 'Detail' column from the DataFrame using the drop() method, which takes the column name to be 
#dropped as an argument, and the axis=1 parameter specifies that the column should be dropped along the 
#columns axis (i.e., vertically). The `inplace


# In[25]:


print(tobacco_business_data.isnull().sum())


# In[26]:


#tobacco_business_data.dropna(subset=['Longitude', 'Latitude', 'Location'], inplace=True)

##there are only 10 missing values for longitude, latitude, and location columns, we can simply drop the rows with 
#missing values using the dropna() function
#This will remove the rows with missing values for the three columns specified and modify the original dataframe in place.


# In[27]:


print(tobacco_business_data.isnull().sum())


# In[28]:


tobacco_business_data.shape


# In[29]:


# lines of code create two box plots to visualize the distribution and identify any potential outliers in the 'Longitude' and 'Latitude' columns of the dataset.
#A box plot is a standardized way of displaying the distribution of data based on the five-number summary of a dataset (minimum, first quartile, median, third quartile, and maximum). The box plot displays these statistics in a way that allows you to identify any outliers in the data.
#In these specific plots, the x-axis represents the 'Longitude' or 'Latitude' column, while the y-axis represents the values for each observation in the column. Outliers are represented as individual data points beyond the whiskers of the box plot. Any data point beyond the whiskers may be considered an outlier and may warrant further investigation.
#The first line of code creates a box plot for the 'Longitude' column, while the second line creates a box plot for the 'Latitude' column. The 'data' argument specifies the dataframe that contains the columns to be plotted. The 'x' argument specifies the column to be plotted on the x-axis. The 'plt.show()' function displays the resulting plot.


# In[30]:


# Check for outliers
import seaborn as sns
import matplotlib.pyplot as plt

sns.boxplot(data=tobacco_business_data, x='Longitude')
plt.show()


# In[31]:


sns.boxplot(data=tobacco_business_data, x='Latitude')
plt.show()


# In[32]:


#code is performing data normalization or scaling on two numerical columns, 'Longitude' and 'Latitude'.
#The first step is to import the StandardScaler class from the sklearn.preprocessing module, which provides a simple way to normalize or scale data.
#Then, a new instance of the StandardScaler class is created and stored in the variable 'scaler'.
#The variable 'num_cols' contains a list of the column names to be normalized or scaled, which are 'Longitude' and 'Latitude' in this case.
#Finally, the fit_transform() method of the scaler object is called on the 'num_cols' columns of the dataframe 'df'. This method first fits the scaler object to the data, computing the mean and standard deviation of the columns, and then applies the transformation to the same columns, rescaling their values so that they have a mean of zero and a standard deviation of 1. The transformed values are then assigned back to the same columns of the original dataframe 'df'.


# In[33]:


# Normalize or scale the data
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
num_cols = ['Longitude', 'Latitude']
tobacco_business_data[num_cols] = scaler.fit_transform(tobacco_business_data[num_cols])


# In[34]:


# code encodes the categorical variables in the dataframe using one-hot encoding technique. It does the following:
#Creates a list of categorical columns to be encoded i.e., License Type, License Status, Industry, and Address Borough.
#Iterates through each categorical column in the list using a for loop.
#Uses the pd.get_dummies() function to create dummy variables for each categorical column. This function creates a new dataframe with a column for each unique value in the categorical column and assigns a binary value of 0 or 1 to each row in the column based on whether the row belongs to that value or not.
#The prefix parameter is used to add a prefix to the new column names created for the dummy variables to identify them easily.
#The pd.concat() function is used to concatenate the original dataframe with the new dummy variable columns created.
#Finally, the original categorical column is dropped from the dataframe using the drop() function.
#By the end of this code, the original categorical columns in the dataframe are replaced by their respective one-hot encoded columns.


# In[35]:


# Encode categorical data
#cat_cols = ['License Type', 'License Status', 'Industry', 'Address Borough']
#for col in cat_cols:
#   tobacco_business_data = pd.concat([tobacco_business_data, pd.get_dummies(tobacco_business_data[col], prefix=col)], axis=1)
#  tobacco_business_data.drop([col], axis=1, inplace=True)


# In[36]:


#code are used to visualize the data in different ways.
#sns.countplot(data=df, x='License Status'): This line creates a count plot that displays the number of active and inactive licenses in the dataset.
#sns.histplot(data=df, x='Longitude', hue='License Status', kde=True): This line creates a histogram plot that shows the distribution of longitude values for active and inactive licenses. The hue parameter is used to distinguish between active and inactive licenses, and the kde parameter is set to True to add a kernel density estimate to the plot.
#sns.scatterplot(data=df, x='Longitude', y='Latitude', hue='License Status'): This line creates a scatter plot that displays the locations of businesses with active and inactive licenses on a map. The x and y parameters are used to set the longitude and latitude axes, respectively. The hue parameter is used to distinguish between active and inactive licenses.


# In[37]:


tobacco_business_data['License Status'].unique()


# In[38]:


# Visualize the data
sns.countplot(data=tobacco_business_data, x='License Status')
plt.show()


# In[39]:


sns.histplot(data=tobacco_business_data, x='Longitude', hue='License Status', kde=True)
plt.show()


# In[40]:


sns.scatterplot(data=tobacco_business_data, x='Longitude', y='Latitude', hue='License Status')
plt.show()


# In[41]:


tobacco_business_data.columns


# In[42]:


#Distribution of License Types:
sns.countplot(data=tobacco_business_data, x='License Type')
plt.show()

#plot shows the distribution of license types issued by the DCA. It can give an idea of which type of license is more common and can help in identifying any trends


# In[43]:


#Top 10 Industries with Most Licenses:

tobacco_business_data['Industry'].value_counts()[:10].plot(kind='bar')
plt.title('Top 10 Industries with Most Licenses')
plt.xlabel('Industry')
plt.ylabel('Number of Licenses')
plt.show()


#plot shows the top 10 industries with the most licenses issued. It can help in identifying the most popular industries and any potential areas of growth.


# In[44]:


#License Expiration by Year:

tobacco_business_data['License Expiration Year'] = pd.DatetimeIndex(tobacco_business_data['License Expiration Date']).year
sns.countplot(data=tobacco_business_data, x='License Expiration Year')
plt.title('License Expiration by Year')
plt.xlabel('Year')
plt.ylabel('Number of Licenses')
plt.show()


#plot shows the number of licenses set to expire in each year. It can help in identifying any potential renewal trends.


# In[45]:


#Business Name Word Cloud:

get_ipython().system('pip install wordcloud')

from wordcloud import WordCloud, STOPWORDS

text = ' '.join(tobacco_business_data['Business Name'].dropna().tolist())

wordcloud = WordCloud(width=800, height=400, stopwords=STOPWORDS, background_color='white').generate(text)

plt.figure(figsize=(12, 10))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()


#plot shows a word cloud of the most common words found in the business names of the DCA issues licenses. It can help in identifying any popular or commonly used business names.


# In[46]:


import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://dap_user:Dap_123456789@87.44.4.77:3306/newyork_businesses")

tablename = "preprocessed_tobocco_business_Data"

tobacco_business_data.to_sql(tablename, con=engine, if_exists="append", index=False)


# In[ ]:





# In[ ]:





# In[ ]:




