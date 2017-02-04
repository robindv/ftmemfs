import logging
import inspect

import ftmemfs.etc.config as config

logging.basicConfig(
	format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')


class WeaselLogger(object):
	log_levels = {'DEBUG': logging.DEBUG,
				  'INFO': logging.INFO,
				  'ERROR': logging.ERROR
				  }

	def __init__(self, name, path):
		self.path = path
		fh = logging.FileHandler(path)
		fmt = logging.Formatter('%(asctime)-2s: %(levelname)s -\
                                     - %(message)s')
		fh.setFormatter(fmt)
		self.file_handler = fh
		self.local_logger = logging.getLogger(name)
		self.local_logger.addHandler(fh)
		self.local_logger.propagate = False
		self.local_logger.setLevel(WeaselLogger.log_levels[config.LOG_LEVEL])

	def __del__(self):
		try:
			while len(self.local_logger.handlers) > 0:
				h = self.local_logger.handlers[0]
				h.close()
				self.local_logger.removeHandler(h)
		except:
			pass

	def error(self, message):
		daddy = inspect.stack()[2][3]
		new_message = "[" + daddy + "] - " + str(message)
		self.local_logger.error(new_message)

	def exception(self, message):
		daddy = inspect.stack()[2][3]
		new_message = "[" + daddy + "] - " + str(message)
		self.local_logger.exception(new_message)

	def info(self, message):
		daddy = inspect.stack()[2][3]
		new_message = "[" + daddy + "] - " + str(message)
		self.local_logger.info(new_message)

	def debug(self, message):
		daddy = inspect.stack()[2][3]
		new_message = "[" + daddy + "] " + str(message)
		self.local_logger.debug(new_message)
