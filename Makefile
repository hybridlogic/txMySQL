.PHONY: trialtests pyflakes default

default:
	@echo consider some of:
	@echo '    ' make trialtests pyflakes 

pyflakes: export PYTHONPATH:= txmysql:test
pyflakes:
	pyflakes src/. | sort

trialtests: export PYTHONPATH:= txmysql:test
trialtests:
	mkdir -p _logs_temp
	trial --rterrors test.test_txmysql.MySQLClientTest

keyerror: export PYTHONPATH:= txmysql:test
keyerror:
	mkdir -p _logs_temp
	trial --rterrors test.test_txmysql.MySQLClientTest.test_0900_autoRepairKeyError
