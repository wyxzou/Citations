#!/usr/bin/env python
# coding: utf-8

# In[2]:


from utility import EnglishTextProcessor


# In[3]:


etp = EnglishTextProcessor()


# In[4]:


from gensim.models import FastText


# In[5]:


import pickle


# In[9]:


vec = pickle.load(open('vec.model', 'rb'))
fasttext = FastText.load('fasttext.model')


# In[ ]:




