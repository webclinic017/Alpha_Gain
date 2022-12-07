from collections import OrderedDict
  
# initializing dictionary
test_dict = {'gfg' : 4, 'is' : 2, 'best' : 5}
print(type(test_dict))

# printing original dictionary
print("The original dictionary : " + str(test_dict))
  
# Reverse Dictionary Keys Order
# Using OrderedDict() + reversed() + items()
res = OrderedDict(reversed(list(test_dict.items())))
  
# printing result 
print("The reversed order dictionary : " + str(res)) 