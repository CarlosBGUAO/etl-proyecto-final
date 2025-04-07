# Proyecto Final ETL – Análisis de Penetración de Créditos en Factura de Gas

Este repositorio contiene el código fuente, análisis exploratorio y visualizaciones para un pipeline ETL enfocado en el análisis de penetración de créditos en la factura de gas. Se utilizan herramientas como Python, Pandas, SQLite, Prefect y visualización con Matplotlib/Seaborn.


---

## ⏱️ Automatización con Prefect

Este proyecto está preparado para ejecutarse automáticamente usando [Prefect](https://www.prefect.io/).

### 🧪 Programación diaria

El flujo `etl_pipeline` se ejecuta todos los días a las **6:00 AM** (hora Colombia), según el cron definido en el archivo `prefect.yaml`.

```yaml
cron: "0 6 * * *"
timezone: "America/Bogota"
```

### ⚙️ Configuración simulada para producción (`prefect.yaml`)

El archivo de despliegue (`prefect.yaml`) simula un entorno de producción con credenciales ficticias y está listo para ser usado con:

```bash
prefect deploy --name etl-penetracion-creditos
```

El flujo puede ser monitoreado y orquestado desde Prefect Cloud con:

```bash
prefect orion start
```

---

