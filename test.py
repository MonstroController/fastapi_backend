import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Пример данных
data = {
    'hour': ['10:00', '11:00', '12:00', '13:00'],
    'to_working': [100, 120, 80, 90],
    'to_overtime': [20, 30, 25, 40]
}
df = pd.DataFrame(data)

# Построение графика
plt.figure(figsize=(8, 5))
plt.plot(df['hour'], df['to_working'], label='to_working', marker='o')
plt.plot(df['hour'], df['to_overtime'], label='to_overtime', marker='s')
plt.title('Статистика операций по часам')
plt.xlabel('Час')
plt.ylabel('Количество операций')
plt.legend()
plt.grid(True)
plt.show()