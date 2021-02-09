
class GForm(object):
	"""docstring for GForm"""
	def __init__(self, arg):
		super(GForm, self).__init__()
		self.arg = arg
		self.title = self.arg['title']
		
	def set_title(self):
		block = '// create & name Form \n var item = "{}"; \n var form = FormApp.create(item).setTitle(item);\n'
		return block.format(self.title)

	def single_line(self):
		return

	def multiline(self):
		return

	def radiobuttons(self):
		return

	def checkbox(self):
		return


arg = {'title': 'test'}


print(GForm(arg).set_title())