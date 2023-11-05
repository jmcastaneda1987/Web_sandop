import pandas as pd
import numpy as np
import os

class Reporte():

    def __init__(self):
        self.mes_de_corte = 6
        self.data_rolling = pd.DataFrame()
        self.data_complementaria = pd.DataFrame()
        self.data_clientes = pd.DataFrame()
        self.data_diccionario = pd.DataFrame()
        self.resultado = pd.DataFrame()

    def calcular(self):
        self.data_rolling['fecha_formatted'] = pd.to_datetime(self.data_rolling['Mes, Año de Fecha'], errors='coerce')
        self.data_rolling['mes'] = self.data_rolling['fecha_formatted'].dt.month
        self.data_rolling['Version'] = self.data_rolling['Version'].str.upper()

        self.data_complementaria['fecha'] = pd.to_datetime(self.data_complementaria['Fecha de facturación'], format='%d.%m.%Y')
        self.data_complementaria['mes'] = self.data_complementaria['fecha'].dt.month
        for indice, carro in self.data_diccionario.iterrows():
            self.data_complementaria['Modelo'] = self.data_complementaria['Modelo'].apply(
                lambda x: carro['Homologado'] if x == carro['Nombre'] else x)

        self.data_clientes['Canal'] = np.where(self.data_clientes['Descripcion Local Dealer'].str.contains("DCO"),
                                        "Retail_red_propia", "Retail_red_tercera")
        self.data_clientes['fecha'] = pd.to_datetime(self.data_clientes['Fecha de Factura'], format='%d.%m.%Y')
        self.data_clientes['mes'] = self.data_clientes['fecha'].dt.month
        self.data_clientes['Material.'] = self.data_clientes['Material.'].str.upper()
        for indice, carro in self.data_diccionario.iterrows():
            self.data_clientes['Material.'] = self.data_clientes['Material.'].apply(
                lambda x: carro['Homologado'] if x == carro['Nombre'] else x)

        venta_red_propia_marca_principal = self.data_complementaria.groupby(['mes', 'Sector', 'Modelo'])[
            'Cantidad de Unidades'].sum().reset_index()
        venta_red_propia_marca_principal = venta_red_propia_marca_principal.rename(
            columns={'Cantidad de Unidades': 'Red_propia_critroen'})

        venta_retail = self.data_clientes.groupby(['mes', 'Sector', 'Material.', 'Canal'])['Cantidad'].sum().reset_index()
        venta_retail = venta_retail.pivot_table(index=['mes', 'Sector', 'Material.'], columns='Canal',
                                                values='Cantidad', aggfunc='sum').reset_index()
        venta_retail.fillna(0, inplace=True)
        nuevos_nombres = ["mes", "Sector", "Modelo", "Retail_red_propia_derco", "Retail_red_tercera"]
        venta_retail.columns = nuevos_nombres

        ventas_reales = pd.merge(venta_retail, venta_red_propia_marca_principal, on=['mes', 'Modelo'], how='outer')
        ventas_reales['Retail_red_propia'] = np.where(ventas_reales['Sector_x'] == "Citroen",
                                                      ventas_reales['Red_propia_critroen'],
                                                      ventas_reales['Retail_red_propia_derco'])

        columnas = ['mes', 'Sector_x', 'Modelo', 'Retail_red_propia', 'Retail_red_tercera']
        ventas_reales.fillna(0, inplace=True)
        ventas_reales = ventas_reales[columnas]
        nuevos_nombres = ['mes', 'Sector', 'Version', 'Retail_red_propia', 'Retail_red_tercera']
        ventas_reales.columns = nuevos_nombres
        ventas_reales['Ventas_total_retail'] = ventas_reales['Retail_red_propia'] + ventas_reales['Retail_red_tercera']

        rundown = self.data_rolling[~self.data_rolling['Version'].str.contains("TOTAL", case=False)]
        rundown = pd.merge(rundown, ventas_reales, on=['mes', 'Version'], how='left')
        rundown['Wh_venta_red_propia'] = rundown['Retail_red_propia']
        rundown['Wh_venta_red_tercera'] = rundown['Invoice total stock transfered to dealer'] - rundown[
            'Wh_venta_red_propia']
        rundown.fillna(0, inplace=True)

        rundown_por_version = rundown[rundown['mes'] <= self.mes_de_corte]
        rundown_por_version = rundown_por_version.groupby(['País', 'Marca', 'mes', 'Version']).agg(
            rrt=('Retail_red_tercera', 'sum'),
            rrp=('Retail_red_propia', 'sum'),
            whrp=('Wh_venta_red_propia', 'sum'),
            whrt=('Wh_venta_red_tercera', 'sum'),
            Total_retail=('Ventas_total_retail', 'sum'),
            total_wh=('Invoice total stock transfered to dealer', 'sum')
        ).reset_index()
        rundown_por_version = rundown_por_version.sort_values(by=['Version', 'mes'])

        rundown_porcentajes_por_version = rundown_por_version.groupby('Version').agg(
            rrt=('rrt', 'sum'),
            rrp=('rrp', 'sum'),
            whrp=('whrp', 'sum'),
            whrt=('whrt', 'sum'),
            Total_retail=('Total_retail', 'sum'),
            total_wh=('total_wh', 'sum')
        ).reset_index()
        rundown_porcentajes_por_version['porc_rrt'] = np.where(
            rundown_porcentajes_por_version['Total_retail'] == 0,
            0,
            round(rundown_porcentajes_por_version['rrt'] / rundown_porcentajes_por_version['Total_retail'], 2))
        rundown_porcentajes_por_version['porc_rrp'] = np.where(
            rundown_porcentajes_por_version['Total_retail'] == 0,
            0,
            round(rundown_porcentajes_por_version['rrp'] / rundown_porcentajes_por_version['Total_retail'], 2))
        rundown_porcentajes_por_version['porc_whrp'] = np.where(
            rundown_porcentajes_por_version['total_wh'] == 0,
            0,
            rundown_porcentajes_por_version['porc_rrt'])
        rundown_porcentajes_por_version['porc_whrt'] = np.where(
            rundown_porcentajes_por_version['total_wh'] == 0,
            0,
            round(1 - rundown_porcentajes_por_version['porc_rrt'], 2))

        rundown_pronosticos = rundown[rundown['mes'] > self.mes_de_corte]
        rundown_pronosticos = rundown_pronosticos[~rundown_pronosticos['Version'].str.contains("TOTAL", case=False)]
        rundown_pronosticos = pd.merge(rundown_pronosticos, rundown_porcentajes_por_version, on=['Version'])
        rundown_pronosticos['Retail_red_propia'] = round(
            rundown_pronosticos['Total Market End Costumer Sales'] * rundown_pronosticos['porc_rrp'])
        rundown_pronosticos['Retail_red_tercera'] = round(
            rundown_pronosticos['Total Market End Costumer Sales'] * rundown_pronosticos['porc_rrt'])
        rundown_pronosticos['Wh_venta_red_propia'] = round(
            rundown_pronosticos['Invoice total stock transfered to dealer'] * rundown_pronosticos['porc_whrp'])
        rundown_pronosticos['Wh_venta_red_tercera'] = round(
            rundown_pronosticos['Invoice total stock transfered to dealer'] * rundown_pronosticos['porc_whrt'])
        rundown_pronosticos['Ventas_total_retail'] = rundown_pronosticos['Retail_red_propia'] + rundown_pronosticos[
            'Retail_red_tercera']
        columnas = ['Version', 'mes', 'Product Code', 'Total_retail', 'total_wh', 'rrt', 'rrp', 'whrp', 'whrt',
                    'porc_rrt', 'porc_rrp',
                    'porc_whrp', 'porc_whrt']

        columnas_numericas = rundown.select_dtypes(include=['number']).columns
        columnas_numericas = columnas_numericas.difference(['mes'])
        rundown_totales = rundown.groupby(['País', 'Marca', 'mes', 'Modelo'])[columnas_numericas].sum().reset_index()
        rundown_totales['Version'] = rundown_totales['Modelo'] + ' Total'
        rundown_totales['Product Code'] = rundown_totales['Modelo'] + ' ' + rundown_totales['Modelo'] + ' Total'
        rundown_totales['mes'] = rundown_totales['mes'].astype(int)
        rundown_totales['fecha_formatted'] = pd.to_datetime(rundown_totales['mes'].astype(str) + '-01-2023',
                                                            format='%m-%d-%Y')
        rundown_totales['Mes, Año de Fecha'] = '2023-' + rundown_totales['mes'].astype(str)

        rundown_final = rundown[rundown['mes'] <= self.mes_de_corte]
        rundown_final = pd.concat([rundown_final, rundown_pronosticos], ignore_index=True)
        rundown_final = pd.concat([rundown_final, rundown_totales], ignore_index=True)
        columnas = ["País", "Modelo", "Version", "Product Code", "fecha_formatted", "Production Request",
                    "Allocation", "Arrivals", "Shipment", "Invoice total stock transfered to dealer",
                    "Landed Distributor Stock",
                    "Dealer Stock", "Production", "Total Market End Costumer Sales", "Wh_venta_red_propia",
                    "Wh_venta_red_tercera", "Retail_red_propia", "Retail_red_tercera", "porc_rrt",
                    "porc_rrp", "porc_whrp", "porc_whrt", "Ventas_total_retail"]
        rundown_final = rundown_final[columnas]

        self.resultado = rundown_final


