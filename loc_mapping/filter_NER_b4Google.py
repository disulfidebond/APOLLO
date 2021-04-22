import re

bus_list = []
with open('parsed_addresses.04192021.csv') as fOpen:
  for i in fOpen:
    i = i.rstrip('\r\n')
    bus_list.append(i)


gList = []
with open('list4GooglePlaces.1858_042021.txt') as fOpen:
  for i in fOpen:
    i = i.rstrip('\r\n')
    gList.append(i)

for i in gList:
  iSplit = i.split(',')
  checkTerm = iSplit[1]
  foundMatch = False
  if checkTerm in bus_list:
    # print('found exact match for ' + str(i))
    pass
  else:
    for x in bus_list:
      m = re.search(checkTerm,x)
      if m:
        # print('found regex match for ' + str(checkTerm))
        foundMatch = True
        break
    if not foundMatch:
      s_len = len(checkTerm)
      s_pad_fwd = int(s_len/2) + 2
      s_pad_rev = int(s_len/2) - 2
      s1 = checkTerm[0:s_pad_fwd]
      s2 = checkTerm[s_pad_rev:]
      for x in bus_list:
        m1 = re.search(s1, x)
        m2 = re.search(s2, x)
        if m1 and ' ' in s1:
          foundMatch = True
          # print('found windowed regex for ' + str(checkTerm))
          break
        if m2 and ' ' in s2:
          foundMatch = True
          # print('found windowed regex for ' + str(checkTerm))
          break
    if not foundMatch:
      print('no match for ' + str(i))
