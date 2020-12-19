import pandas as pd

class thisDF(pd.DataFrame):

	@property
	def _constructor(self):
		return thisDF

	_metadata = ['new_property']

	def __init__(self, data=None, index=None, columns=None, copy=False, new_property='reset'):
		
		super(thisDF, self).__init__(data=data, index=index, columns=columns, dtype='str', copy=copy)

		self.new_property = new_property

cols = ['A', 'B', 'C']
new_property = cols[:2]
tdf = thisDF(columns=cols, new_property=new_property)

print(tdf.new_property)

tdf = tdf.append(pd.Series(['a', 'b', 'c'], index=tdf.columns), ignore_index=True)

print(tdf.new_property)