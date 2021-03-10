# -*- coding: utf-8 -*-
"""
Created on Sun Mar  7 00:08:24 2021

@author: User
"""
import pandas as pd
data=pd.read_json('contacts.json')
data['email_len']=data['Email'].apply(lambda x:len(x))
data['phone_len']=data['Phone'].apply(lambda x:len(x))
data['order_len']=data['OrderId'].apply(lambda x:len(x))
email=data[data['email_len']!=0][['Email','Id']]
order=data[data['order_len']!=0][['OrderId','Id']]
phone=data[data['phone_len']!=0][['Phone','Id']]
ep=data[(data['email_len']!=0)&(data['phone_len']!=0)][['Email','Phone','Id']]
eo=data[(data['email_len']!=0)&(data['order_len']!=0)][['Email','OrderId','Id']]
po=data[(data['phone_len']!=0)&(data['order_len']!=0)][['Phone','OrderId','Id']]
eoe=pd.merge(eo,email,how='right',on=['Email'])
eoo=pd.merge(eo,order,how='right',on=['OrderId'])
epe=pd.merge(ep,email,how='right',on=['Email'])
epp=pd.merge(ep,phone,how='right',on=['Phone'])
pop=pd.merge(po,phone,how='right',on=['Phone'])
poo=pd.merge(po,order,how='right',on=['OrderId'])

eoe_epe=pd.merge(eoe,epe,how='outer',on=['Email'])
eoo_poo=pd.merge(eoo,poo,how='outer',on=['OrderId'])
epp_pop=pd.merge(epp,pop,how='outer',on=['Phone'])

id1=eoe_epe[[ 'Id_x_x', 'Id_y_x','Id_x_y', 'Id_y_y']]
id2=eoo_poo[[ 'Id_x_x', 'Id_y_x','Id_x_y', 'Id_y_y']]
id3=epp_pop[[ 'Id_x_x', 'Id_y_x','Id_x_y', 'Id_y_y']]
id_df=pd.concat([id1,id2,id3])


def is_missing(col1,col2,col3,col4):    
    if pd.isnull(col1):        
        if pd.isnull(col2):
            if pd.isnull(col3):
                 return col4
            else : 
                return col3
        else : 
            return col2
        
    else:
        return col1

id_df['Id_x_x']= id_df.apply(lambda x: is_missing(x['Id_x_x'],x['Id_y_x'],x['Id_x_y'],x['Id_y_y']),axis=1)
id_df['Id_x_y']= id_df.apply(lambda x: is_missing(x['Id_x_y'],x['Id_y_x'],x['Id_x_x'],x['Id_y_y']),axis=1)
id_df['Id_y_x']= id_df.apply(lambda x: is_missing(x['Id_y_x'],x['Id_x_x'],x['Id_x_y'],x['Id_y_y']),axis=1)
id_df['Id_y_y']= id_df.apply(lambda x: is_missing(x['Id_y_y'],x['Id_y_x'],x['Id_x_y'],x['Id_x_x']),axis=1)
id1=id_df[['Id_x_x','Id_x_y']]
id2=id_df[['Id_y_x','Id_y_y']]
id1.rename(columns = {'Id_x_x': 'id1', 'Id_x_y': 'id2'}, inplace = True)
id2.rename(columns = {'Id_y_x': 'id1', 'Id_y_y': 'id2'}, inplace = True)
id_final=pd.concat([id1,id2])
import networkx as nx

id_final=list(id_final.to_records(index=False))

G=nx.from_edgelist(id_final)
l=list(nx.connected_components(G))
add_list=[]
for s in l:
    li=list(s)
    add_list.append(li)
r=pd.Series(add_list)
result=pd.DataFrame(r)
result['list_len']=result[0].apply(lambda x: len(x))
sum(result['list_len'])



contact=data[['Contacts','Id']]
contact=pd.Series(contact.Contacts,index=contact.Id).to_dict()


#get contact sum

def find_dict(x):
    summation=0
    for n in x:
        add=contact[n]
        summation=summation+add
    return summation

result['contacts']=result[0].apply(lambda x: find_dict(x))

final_dict=dict()
result_list=result[0].to_list()
for li in result_list:
    for element in li:
        final_dict[element]=li
        
final_result= pd.DataFrame(list(final_dict.items()),columns = ['ticket_id','col2']) 
result.rename(columns = {0: 'col2'}, inplace = True)
def list_to_string(x):
    x=sorted(x)
    a='-'.join(str(int(i)) for i in x)
    return a


result['col3']=result['col2'].apply(lambda x:list_to_string(x))
final_result['col3']=final_result['col2'].apply(lambda x:list_to_string(x))
final=pd.merge(final_result[['ticket_id','col3']],result[['col3','contacts']],on=['col3'])
final['contacts']=final['contacts'].astype(str)
final['col3/contact']=final[['col3','contacts']].agg(', '.join,axis=1)   
final.rename(columns = {'col3/contact': 'ticket_trace/contact'}, inplace = True)  

final=final.sort_values(by=['ticket_id'])
final['ticket_id']=final['ticket_id'].astype(int)

final[['ticket_id','ticket_trace/contact']].to_csv('result.csv',index=False)