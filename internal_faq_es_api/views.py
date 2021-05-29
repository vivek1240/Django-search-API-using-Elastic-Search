from django.shortcuts import render
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from django.http import JsonResponse
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_200_OK
import json

"""
def fetch_results(request):
    query_dict= request.GET
    id= query_dict.get('id')
    client= Elasticsearch()
    s= Search(using= client, index='your_index')

    if not id: 
        return JsonResponse({'error':"Invalid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    else:
        if id.isspace():
            return JsonResponse({'error':"Inavlid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    q= Q("term", id=id) 
    s=s.query(q)
    response= s.execute()
    return JsonResponse(response.to_dict(), status= HTTP_200_OK, safe= False)
"""

# Create your views here.
"""
def fetch_results(request):
    query_dict= request.GET
    id = query_dict.get('id')
    client= Elasticsearch()
    s= Search(using= client, index='your_index')

    if not id: 
        return JsonResponse({'error':"Invalid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    else:
        if id.isspace():
            return JsonResponse({'error':"Inavlid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    q= Q("match", id=id) 
    s=s.query(q)
    response= s.execute()
    #print(response)
    #print("HERE")
    return JsonResponse(response.to_dict(), status= HTTP_200_OK, safe= False)
    """



def fetch_results(request):
    query_dict= request.GET   #this request will be a string given by the user 
    question = query_dict.get('question') 
    client= Elasticsearch()
    s= Search(using= client, index='your_index')

    if not question: 
        return JsonResponse({'error':"Invalid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    else:
        if question.isspace():
            return JsonResponse({'error':"Inavlid or Empty field"}, status= HTTP_400_BAD_REQUEST)

    q= Q("match", question = question)
    s=s.query(q)
    response= s.execute()
    search = get_results(response) #response is dicitonary type
    #reponse_final = json.dumps(search)
    return JsonResponse(search, status= HTTP_200_OK, safe= False)


def get_results(response): 
    results = {}  
    for hit in response: 
        #result_tuple = (hit.question + ' ' + hit.answer,
        #hit.id, hit.phrasing, hit.createdat)    
        key= hit.question
        value= hit.answer
        #results[key]= 0
    return results    