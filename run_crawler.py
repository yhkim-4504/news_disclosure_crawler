import sys
from PyQt5 import QtWidgets
from NewsGUI import Gui


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    news = Gui('news.db', 'disclosure.db')
    sys.exit(app.exec())