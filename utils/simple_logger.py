import time


class SimpleLogger(object):

	def __init__(self, path):
		self.path = path

		# Recreate the file.
		f = open(self.path, 'w')
		f.close()

	def log(self, value, add_time=False, reference_time=0):
		f = open(self.path, 'a')
		if add_time:
			log_time = time.time() - reference_time
			f.write(str(log_time) + ',' + str(value) + '\n')
		else:
			f.write(str(value) + '\n')
		f.close()
