import rclpy  # Cliente ROS 2 para Python
from rclpy.node import Node
from std_msgs.msg import String  # Mensajes tipo String (JSON en este caso)
import json
import requests  # Para enviar HTTP POST
from datetime import datetime
from collections import defaultdict  # Para agrupar datos por id_dispositivo
import statistics  # Para calcular medianas

# URL de la API donde se publican los datos filtrados
API_URL = "https://biorreactor-app.onrender.com/api/sensores"

# Función para formatear valores numéricos con 2 decimales, o "N/A" si no válido
def formato_seguro(value):
    return f"{value:.2f}" if isinstance(value, (int, float)) else "N/A"

# Función para calcular la mediana de un campo numérico dentro de una lista de diccionarios
def calcular_mediana(buffer, campo):
    try:
        valores = [d[campo] for d in buffer if campo in d and isinstance(d[campo], (int, float))]
        if not valores:
            return None
        return statistics.median(sorted(valores))
    except Exception:
        return None

# Función para redondear valores numéricos a 2 decimales, o devuelve None
def redondear(valor):
    return round(valor, 2) if isinstance(valor, (int, float)) else None

# Clase del nodo central
class NodoCentral(Node):
    def __init__(self):
        super().__init__('nodo_central')

        # Suscripción al topic "datos_sensores" con mensajes tipo String
        self.subscription = self.create_subscription(
            String,
            'datos_sensores',
            self.listener_callback,
            10
        )
        # Diccionario con una lista de datos por id_dispositivo
        self.buffers_por_dispositivo = defaultdict(list)  # Cada dispositivo tiene su lista de datos
        
        # Timer que se ejecuta cada 60 segundos para intentar publicar si es el momento
        self.timer = self.create_timer(60.0, self.publicar_datos)

    def listener_callback(self, msg):
        try:
            data = json.loads(msg.data)
            id_disp = data.get("id_dispositivo")
            if id_disp:
                self.buffers_por_dispositivo[id_disp].append(data)

                # Log de los datos recibidos
                self.get_logger().info(
                    f"✅ Dato recibido de 🆔 {id_disp} en 🌐 {data.get('dominio')}\n"
                    f"🌡️ Temperatura: {formato_seguro(data.get('temperatura'))} °C | "
                    f"☀️ Luz: {formato_seguro(data.get('luz'))} lux \n"
                    f"🌊 pH: {formato_seguro(data.get('ph'))} | "
                    f"⚡ Voltaje pH: = {formato_seguro(data.get('voltaje_ph'))} V \n"
                    f"🧪 Turbidez: {formato_seguro(data.get('turbidez'))} % | "
                    f"⚡ Voltaje turbidez: = {formato_seguro(data.get('voltaje_turb'))} V \n"
                    f"🫁 Oxígeno: {formato_seguro(data.get('oxigeno'))} mg/L | "
                    f"⚡ Voltaje oxígeno: = {formato_seguro(data.get('voltaje_o2'))} V \n"
                )
            else:
                self.get_logger().warn("⚠️ Mensaje recibido sin 'id_dispositivo'")
        except json.JSONDecodeError:
            self.get_logger().warn("⚠️ Mensaje no es JSON válido:")
            self.get_logger().warn(msg.data)
        except Exception as e:
            self.get_logger().error(f"❌ Error procesando mensaje: {e}")

    def publicar_datos(self):
        ahora = datetime.now()
        minuto = ahora.minute

        # Solo publica en el minuto 0 o 30
        if minuto in [0, 30]:
            self.get_logger().info(f"⏱️ Publicación datos medianos a las {ahora.strftime('%H:%M')}")

            for id_disp, buffer in self.buffers_por_dispositivo.items():
                if not buffer:
                    continue

                # Prepara el diccionario de datos a enviar
                datos_filtrados = {
                    "id_dispositivo": id_disp,
                    "dominio": buffer[-1].get("dominio", "desconocido"),
                    "temperatura": redondear(calcular_mediana(buffer, "temperatura")),
                    "ph": redondear(calcular_mediana(buffer, "ph")),
                    "turbidez": redondear(calcular_mediana(buffer, "turbidez")),
                    "oxigeno": redondear(calcular_mediana(buffer, "oxigeno")),
                    "luz": redondear(calcular_mediana(buffer, "luz")),
                }

                # Envío HTTP POST a la API
                try:
                    response = requests.post(API_URL, json=datos_filtrados, timeout=5)
                    if response.status_code == 201:
                        self.get_logger().info(f"[{ahora.strftime('%H:%M')}] ✅ Datos de {id_disp} enviados")
                    else:
                        self.get_logger().warn(f"[{ahora.strftime('%H:%M')}] ⚠️ Error al enviar {id_disp}: {response.status_code} - {response.text}")
                except requests.RequestException as e:
                    self.get_logger().error(f"❌ Error al enviar datos de {id_disp}: {e}")

                # Mostrar por consola lo enviado
                self.get_logger().info(
                    f"🆔 {id_disp} | Temperatura: {formato_seguro(datos_filtrados['temperatura'])}°C | "
                    f"pH: {formato_seguro(datos_filtrados['ph'])} | Turbidez: {formato_seguro(datos_filtrados['turbidez'])}% | "
                    f"Oxígeno: {formato_seguro(datos_filtrados['oxigeno'])}mg/L | Luz: {formato_seguro(datos_filtrados['luz'])}lux"
                )

                # Limpia el buffer para ese dispositivo después de publicar
                self.buffers_por_dispositivo[id_disp] = []
        else:
            self.get_logger().debug(f"[{ahora.strftime('%H:%M')}] ⏳ Esperando próximo intervalo de publicación......")

# Función principal del nodo
def main(args=None):
    rclpy.init(args=args)
    nodo = NodoCentral()
    rclpy.spin(nodo)
    nodo.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
