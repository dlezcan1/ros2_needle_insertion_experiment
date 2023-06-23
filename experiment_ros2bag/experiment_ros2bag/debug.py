 
import pandas as pd
import numpy as np



# df=pd.read_csv('axes_positions2.csv')
## print(df.groupby(["1", "2", "3", "4"]).size().reset_index().rename(columns={0:'count'}).sort_values(by=['count']))
## print(df)
# print(df[ df['4']<-90.0 ])

# inserted=df[  (df['2']==60.0) & (df['3']==0.0) & (df['4']==-90.0)  ] 
## print(np.array(inserted["0"])[0])
## print(np.array(inserted["0"])[10])
## print(np.array(inserted["0"])[20])
## print(np.array(inserted["0"])[100])


# df2=pd.read_csv('needle_shapes.csv')
## shapes_inserted=df2[ (df2['0']<np.array(inserted["0"])[10]+1000000000) & (df2['0']>np.array(inserted["0"])[100]-1000000000) ]
# shapes_inserted=df2[2000:2001]

# print(shapes_inserted)
# shape= np.array(shapes_inserted['1'][:1])  

# str1=shape[0]
## print( type(shape[0]) )

# print(str1)

## list_shapes=str1.strip('][').split(', ')
### print(   list_shapes   )

## y_values=[]
## i=1
## while (i<len(list_shapes)):
## 	y_values.append(list_shapes[i])
## 	i+=3

## y_values=list(map(float, y_values))
## print(y_values)

## df = pd.DataFrame(y_values)
## df.to_csv("shape_inserted.csv")







df2=pd.read_csv('needle_shapes4.csv')
## shapes_inserted=df2[ (df2['0']<np.array(inserted["0"])[10]+1000000000) & (df2['0']>np.array(inserted["0"])[100]-1000000000) ]
shapes_inserted=df2

# print(shapes_inserted)
shape= np.array(shapes_inserted['1'][:1])  

str1=shape[0]
## print( type(shape[0]) )

# print(str1)

list_shapes=str1.strip('][').split(', ')
# print(list_shapes)

x_values, y_values, z_values = [], [], []
i=0
while (i<len(list_shapes)):
	x_values.append(list_shapes[i][1:])
	y_values.append(list_shapes[i+1])
	z_values.append(list_shapes[i+2][:-1])
	i+=3

x_values=list(map(float, x_values))
y_values=list(map(float, y_values))
z_values=list(map(float, z_values))
print(x_values)
print(y_values)
print(z_values)


df = pd.DataFrame(list(zip(x_values, y_values, z_values)),
               columns =['x', 'y', 'z'])
df.to_csv("shape_inserted4.csv")



