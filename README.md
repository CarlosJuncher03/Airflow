# Airflow, DBT e PostgreSQL: Uma Combinação Poderos
Esta documentação tem como objetivo demonstrar o processo de ELT utilizando ferramentas como Python, Airflow, PostgreSQL e dbt. O foco é apresentar como essas tecnologias se integram para automatizar a extração, carga e transformação de dados de forma eficiente.
<br>
### Introdução da Pipeline

O projeto será desenvolvido em quatro etapas principais:

1. **Extração**: Coletar os dados de ofertas de celulares do Mercado Livre através de requisições à API.
2. **Processamento**: Processar os dados extraídos e armazená-los em um Data Lake.
3. **Transformação**: Aplicar transformações nos dados utilizando o dbt para prepará-los para análise.
4. **Visualização**: Exibir os dados transformados em um dashboard para visualização e análise eficiente.

```diagram
PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2ZXJzaW9uPSIxLjEiIHdpZHRoPSI3MjFweCIgaGVpZ2h0PSI2MXB4IiB2aWV3Qm94PSItMC41IC0wLjUgNzIxIDYxIiBjb250ZW50PSImbHQ7bXhmaWxlIGhvc3Q9JnF1b3Q7ZW1iZWQuZGlhZ3JhbXMubmV0JnF1b3Q7IGFnZW50PSZxdW90O01vemlsbGEvNS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xMjguMC4wLjAgU2FmYXJpLzUzNy4zNiBFZGcvMTI4LjAuMC4wJnF1b3Q7IHZlcnNpb249JnF1b3Q7MjQuNy4xMiZxdW90OyZndDsmbHQ7ZGlhZ3JhbSBpZD0mcXVvdDtYWkVxSEI1SG56MEdGanlxRkZyNiZxdW90OyBuYW1lPSZxdW90O1DDoWdpbmEtMSZxdW90OyZndDsxWlpmYjVzd0ZNVS9EZThFR3RJOUxuL2FQcVRTcGt6cXM0VnZzU2ZEcGVZU1FqOTlUYmlFSUhkYnBVMURmWXI5ODNWOHovRUJFY1NiL0hSdlJha2VVWUlKb2xDZWduZ2JSRkVVSm9uNzZVamJrOXRvMllQTWF0bWp4UWdPK2hVWWhreHJMYUdhRkJLaUlWMU9ZWXBGQVNsTm1MQVdtMm5aTTVycHFhWEl3QU9IVkJpZlBtbEphbEN4R3ZrRDZFd05KeStTTC8xS0xvWmlWbElwSWJHNVF2RXVpRGNXa2ZwUmZ0cUE2Y3diZk9uMzNmMWk5ZEtZaFlJK3N1R20zM0FVcG1adDNCZTFnMWlRVGp0UDBaTENEQXRoZGlOZFc2d0xDZDAvaG00MjF1d1JTd2NYRHY0RW9wWXZVdFNFRGluS0RhLzZYYk9RQ211YmNoL2NHUW1iQVZmRlBlbzZ2TnJHU3U4QmN5RGJ1Z0lMUnBBK1RtOVBjQWl5Uzkzb2t4dXdWZS9iRm5tMlBSQ1Y1NU5lYXFqSWpSN0JwdTV1M1dpdmp4WThXMGZUT2djYXBRa09wVGlMYmR4VDgwR0RqbUFKVHI4Vno2czNuRGgrNUM3elpnendZa2lsdWdwdkV2NjlYY25uU1Zuc3AydzVWOHBpejdhdjJqNGI5NzV3M1hSU1d1ZEJNWHV5Wm8zVzdlZUoxdEtQMW1xdWFDMDkyNzVoUlptRncvYzlwMnU3L3VGWitZY3dUWlAzTDE1YWMwWnI1WG0wRlpWYW83QnlkbU9TLzJpTW00NGZKT2UxcTgrNmVQY0cmbHQ7L2RpYWdyYW0mZ3Q7Jmx0Oy9teGZpbGUmZ3Q7Ij48ZGVmcy8+PGc+PGcgZGF0YS1jZWxsLWlkPSIwIj48ZyBkYXRhLWNlbGwtaWQ9IjEiPjxnIGRhdGEtY2VsbC1pZD0iNCI+PGc+PHBhdGggZD0iTSAxMjAgMzAgTCAxOTMuNjMgMzAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDE5OC44OCAzMCBMIDE5MS44OCAzMy41IEwgMTkzLjYzIDMwIEwgMTkxLjg4IDI2LjUgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjIiPjxnPjxyZWN0IHg9IjAiIHk9IjAiIHdpZHRoPSIxMjAiIGhlaWdodD0iNjAiIHJ4PSI5IiByeT0iOSIgZmlsbD0icmdiKDI1NSwgMjU1LCAyNTUpIiBzdHJva2U9InJnYigwLCAwLCAwKSIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiIHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogMTE4cHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogMzBweDsgbWFyZ2luLWxlZnQ6IDFweDsiPjxkaXYgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiIHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiBIZWx2ZXRpY2E7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPkh0dHAgcmVxdWVzdCBNZXJjYWRvIExpdnJlPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjYwIiB5PSIzNCIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPkh0dHAgcmVxdWVzdCBNZXJjYWRvLi4uPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iNiI+PGc+PHBhdGggZD0iTSAzMjAgMzAgTCAzOTMuNjMgMzAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDM5OC44OCAzMCBMIDM5MS44OCAzMy41IEwgMzkzLjYzIDMwIEwgMzkxLjg4IDI2LjUgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjMiPjxnPjxyZWN0IHg9IjIwMCIgeT0iMCIgd2lkdGg9IjEyMCIgaGVpZ2h0PSI2MCIgcng9IjkiIHJ5PSI5IiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3QgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSIgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiAxMThweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiAzMHB4OyBtYXJnaW4tbGVmdDogMjAxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5BaXJmbG93IGUgcHl0aG9uPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjI2MCIgeT0iMzQiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5BaXJmbG93IGUgcHl0aG9uPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iOCI+PGc+PHBhdGggZD0iTSA1MjAgMzAgTCA1OTMuNjMgMzAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDU5OC44OCAzMCBMIDU5MS44OCAzMy41IEwgNTkzLjYzIDMwIEwgNTkxLjg4IDI2LjUgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjUiPjxnPjxyZWN0IHg9IjQwMCIgeT0iMCIgd2lkdGg9IjEyMCIgaGVpZ2h0PSI2MCIgcng9IjkiIHJ5PSI5IiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3QgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSIgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiAxMThweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiAzMHB4OyBtYXJnaW4tbGVmdDogNDAxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5Qb3N0Z3JlU1FMIGUgREJUPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjQ2MCIgeT0iMzQiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5Qb3N0Z3JlU1FMIGUgREJUPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iNyI+PGc+PHJlY3QgeD0iNjAwIiB5PSIwIiB3aWR0aD0iMTIwIiBoZWlnaHQ9IjYwIiByeD0iOSIgcnk9IjkiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5IiBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBjZW50ZXI7IGp1c3RpZnktY29udGVudDogdW5zYWZlIGNlbnRlcjsgd2lkdGg6IDExOHB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDMwcHg7IG1hcmdpbi1sZWZ0OiA2MDFweDsiPjxkaXYgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiIHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiBIZWx2ZXRpY2E7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPkRhc2hCb2FyZDwvZGl2PjwvZGl2PjwvZGl2PjwvZm9yZWlnbk9iamVjdD48dGV4dCB4PSI2NjAiIHk9IjM0IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+RGFzaEJvYXJkPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjwvZz48L2c+PC9nPjxzd2l0Y2g+PGcgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ii8+PGEgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMCwtNSkiIHhsaW5rOmhyZWY9Imh0dHBzOi8vd3d3LmRyYXdpby5jb20vZG9jL2ZhcS9zdmctZXhwb3J0LXRleHQtcHJvYmxlbXMiIHRhcmdldD0iX2JsYW5rIj48dGV4dCB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmb250LXNpemU9IjEwcHgiIHg9IjUwJSIgeT0iMTAwJSI+VGV4dCBpcyBub3QgU1ZHIC0gY2Fubm90IGRpc3BsYXk8L3RleHQ+PC9hPjwvc3dpdGNoPjwvc3ZnPg==
```
### HTTP Request: Extração de Dados do Mercado Livre

A extração de dados será realizada na página de ofertas diárias de celulares do Mercado Livre. Utilizaremos a biblioteca **BeautifulSoup** em Python para tratar os dados recebidos e selecionar os campos específicos do HTML.

```diagram
PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2ZXJzaW9uPSIxLjEiIHdpZHRoPSIxMjFweCIgaGVpZ2h0PSIzOTFweCIgdmlld0JveD0iLTAuNSAtMC41IDEyMSAzOTEiIGNvbnRlbnQ9IiZsdDtteGZpbGUgaG9zdD0mcXVvdDtlbWJlZC5kaWFncmFtcy5uZXQmcXVvdDsgYWdlbnQ9JnF1b3Q7TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEyOC4wLjAuMCBTYWZhcmkvNTM3LjM2IEVkZy8xMjguMC4wLjAmcXVvdDsgdmVyc2lvbj0mcXVvdDsyNC43LjEyJnF1b3Q7Jmd0OyZsdDtkaWFncmFtIGlkPSZxdW90O0FsR3FBUDE3bzhqUnZFQW8xb2dFJnF1b3Q7IG5hbWU9JnF1b3Q7UMOhZ2luYS0xJnF1b3Q7Jmd0O3haVkxjNXd3RE1jL0RkY09qKzZqMTkxc20wTXlrNWs5dEQyNm9JQTdCbEVqRnVpbnI0Z0ZMQ0d2bVczYTAxcC9KRnYrU1ZwNzBUNXZ2MWhWWnJlWWdQRkNQMm05Nk1vTHc5QmZyL21uVnpxbmJNT1ZFMUtyRXljRmszRFV2MEZFWDlSYUoxRE5IQW5Sa0M3bllveEZBVEhOTkdVdE5uTzNlelR6VTB1VndrSTR4c29zMWE4Nm9XeTR4V2JTcjBHbjJYQnlzUDdrdnVScWNKYWJWSmxLc0RtVG9vTVg3UzBpdVZYZTdzSDA4QVl1THU3ek0xL0h4Q3dVOUphQTBBV2NsS25sYnJkZ1k4Nkp4UnQ5c2lCcFVqZmNuZU1aTXhzN3pyM3N4ZGhnemR2dG1rd1RIRXNWOTJMRGxXY3RvOXl3RmZCU1RnSkwwRDZiYlRBeTRPWUJ6SUZzeHk0U0VQbUNUZnBtSTJZekZTRVl5R1puQmRpS3BxVHU2Ymp6aElZWFF1ZHBVaCtYSkJKdUNqSFJVb1lwRnNvY0puVm5zUzRTNkRmdzJacDhiaEJMb2ZJVGlEcnBjRlVUUHNXc1AraGxZcHdYMWpZV3IwaEdRdGtVNkt6T1M2NFdqQ0o5bXU5K0NhWFYvNkFFcmFadmZmaUhsVmpmejc1Y3RiTHpnOUdKY1FIWkN6Qks2QjFxUG1ScTYvVzhyYVBIL2VvcUtWR1BpakdtOGFiNlJJdDV2eVlxSDY3d3E0YUtGdVd6R2VZLzZ1cmZqSGM0NThEenZwanY3VHVOOTJZQjVxN2pSaXhlK1A5N0JZaXFTdmYyM091MjcrNzNJRFIyeXQ4bnhPYjBETGxPbXg3ejZQQUgmbHQ7L2RpYWdyYW0mZ3Q7Jmx0Oy9teGZpbGUmZ3Q7Ij48ZGVmcy8+PGc+PGcgZGF0YS1jZWxsLWlkPSIwIj48ZyBkYXRhLWNlbGwtaWQ9IjEiPjxnIGRhdGEtY2VsbC1pZD0iMiI+PGc+PHBhdGggZD0iTSAzMCAyMCBDIDYgMjAgMCA0MCAxOS4yIDQ0IEMgMCA1Mi44IDIxLjYgNzIgMzcuMiA2NCBDIDQ4IDgwIDg0IDgwIDk2IDY0IEMgMTIwIDY0IDEyMCA0OCAxMDUgNDAgQyAxMjAgMjQgOTYgOCA3NSAxNiBDIDYwIDQgMzYgNCAzMCAyMCBaIiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5IiBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBjZW50ZXI7IGp1c3RpZnktY29udGVudDogdW5zYWZlIGNlbnRlcjsgd2lkdGg6IDExOHB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDQwcHg7IG1hcmdpbi1sZWZ0OiAxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5NZXJjYWRvIExpdnJlPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjYwIiB5PSI0NCIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPk1lcmNhZG8gTGl2cmU8L3RleHQ+PC9zd2l0Y2g+PC9nPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSI0Ij48Zz48cGF0aCBkPSJNIDYwIDE2MCBMIDYwIDg2LjM3IiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0ic3Ryb2tlIi8+PHBhdGggZD0iTSA2MCA4MS4xMiBMIDYzLjUgODguMTIgTCA2MCA4Ni4zNyBMIDU2LjUgODguMTIgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjUiPjxnPjxwYXRoIGQ9Ik0gNjAgMjQwIEwgNjAgMjc1IEwgNjAgMzAzLjYzIiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0ic3Ryb2tlIi8+PHBhdGggZD0iTSA2MCAzMDguODggTCA1Ni41IDMwMS44OCBMIDYwIDMwMy42MyBMIDYzLjUgMzAxLjg4IFoiIGZpbGw9InJnYigwLCAwLCAwKSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSIzIj48Zz48cGF0aCBkPSJNIDYwIDE2MCBMIDEwMCAyMDAgTCA2MCAyNDAgTCAyMCAyMDAgWiIgZmlsbD0icmdiKDI1NSwgMjU1LCAyNTUpIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3QgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSIgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiA3OHB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDIwMHB4OyBtYXJnaW4tbGVmdDogMjFweDsiPjxkaXYgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiIHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiBIZWx2ZXRpY2E7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPkh0dHAgcmVxdWVzdDwvZGl2PjwvZGl2PjwvZGl2PjwvZm9yZWlnbk9iamVjdD48dGV4dCB4PSI2MCIgeT0iMjA0IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+SHR0cCByZXF1ZXN0PC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iNyI+PGc+PGVsbGlwc2UgY3g9IjYwIiBjeT0iMzUwIiByeD0iNDAiIHJ5PSI0MCIgZmlsbD0icmdiKDI1NSwgMjU1LCAyNTUpIiBzdHJva2U9InJnYigwLCAwLCAwKSIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiIHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogNzhweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiAzNTBweDsgbWFyZ2luLWxlZnQ6IDIxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5QeXRob248L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iNjAiIHk9IjM1NCIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPlB5dGhvbjwvdGV4dD48L3N3aXRjaD48L2c+PC9nPjwvZz48L2c+PC9nPjwvZz48c3dpdGNoPjxnIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSIvPjxhIHRyYW5zZm9ybT0idHJhbnNsYXRlKDAsLTUpIiB4bGluazpocmVmPSJodHRwczovL3d3dy5kcmF3aW8uY29tL2RvYy9mYXEvc3ZnLWV4cG9ydC10ZXh0LXByb2JsZW1zIiB0YXJnZXQ9Il9ibGFuayI+PHRleHQgdGV4dC1hbmNob3I9Im1pZGRsZSIgZm9udC1zaXplPSIxMHB4IiB4PSI1MCUiIHk9IjEwMCUiPlRleHQgaXMgbm90IFNWRyAtIGNhbm5vdCBkaXNwbGF5PC90ZXh0PjwvYT48L3N3aXRjaD48L3N2Zz4=
```


#### Links Utilizados:
1. [Página 1 - Ofertas de Celulares](https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES)
2. [Página 2 - Ofertas de Celulares](https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=2)
3. [Página 3 - Ofertas de Celulares](https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=3)
4. [Página 4 - Ofertas de Celulares](https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=4)
5. [Página 5 - Ofertas de Celulares](https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=5)


#### Campos a Serem Extraídos:
- **Nome do Produto:** Nome do celular ofertado.
- **Preço da Oferta:** Preço atual do produto em promoção.
- **Preço Fora da Oferta:** Preço original, sem o desconto.
- **Loja Vendedora:** Nome do vendedor responsável pela oferta.
- **Em Oferta Especial:** Indicação se o produto está em uma promoção especial.
- **Frete Full:** Verificação se o produto tem frete grátis ou entrega rápida (Frete Full).
- **Porcentagem de Desconto:** Percentual de desconto aplicado na oferta.
- **Imagem do Produto:** URL da imagem do produto ofertado.


Essa estrutura facilita o entendimento e execução do script, com todos os links e campos claramente definidos para a extração de dados.

### Código em Python para o ELT

O código abaixo é responsável por solicitar a requisição das 5 páginas de ofertas de celulares no Mercado Livre, receber os dados e tratá-los. Em seguida, esses dados são transformados nos campos específicos mencionados anteriormente.

```python
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Lista de URLs das páginas a serem coletadas
urls = [
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=2",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=3",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=4",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=5"
]

# Lista para armazenar as ofertas de todas as páginas
ofertas = []

# Função para identificar a marca do celular
def identificar_marca(nome_produto):
    marcas = ['Samsung', 'Motorola', 'Apple', 'Xiaomi', 'Realme', 'Multilaser']
    for marca in marcas:
        if marca.lower() in nome_produto.lower():
            return marca
    return 'Outros'

# Função para fazer scraping em uma página
def scrape_page(url):
    # Fazer a requisição da página
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    # Encontrar as informações de cada oferta
    produtos = soup.find_all('li', class_='promotion-item')

    # Iterar sobre cada oferta e extrair as informações desejadas
    for produto in produtos:
        # Nome do Produto
        titulo = produto.find('p', class_='promotion-item__title')
        nome_produto = titulo.text if titulo else 'N/A'

        # Identificar a marca
        marca = identificar_marca(nome_produto)

        # Preço atual
        preco_atual = produto.find('span', class_='andes-money-amount__fraction')
        preco_atual_texto = preco_atual.text if preco_atual else 'N/A'

        # Preço antigo
        preco_antigo = produto.find_all('span', class_='andes-money-amount__fraction')
        preco_antigo_texto = preco_antigo[1].text if len(preco_antigo) > 1 else 'N/A'

        # Loja Vendedora
        loja_vendedora = produto.find('span', class_='promotion-item__seller')
        loja_vendedora_texto = loja_vendedora.text if loja_vendedora else 'N/A'

        # Oferta do Dia
        oferta_do_dia = produto.find('span', class_='promotion-item__today-offer-text')
        oferta_do_dia_texto = oferta_do_dia.text if oferta_do_dia else 'SEM OFERTA DO DIA'

        # Frete Full (Verificação do SVG relacionado ao frete full)
        frete_full = produto.find('svg', class_='full-icon')
        frete_full_texto = 'FRETE FULL' if frete_full else 'SEM FRETE FULL'

        # Porcentagem de desconto
        porcentagem_desconto = produto.find('span', class_='promotion-item__discount-text')
        porcentagem_desconto_texto = porcentagem_desconto.text if porcentagem_desconto else 'SEM DESCONTO'

        # URL da Imagem do Produto
        imagem_produto = produto.find('img', class_='promotion-item__img')
        imagem_produto_url = imagem_produto['src'] if imagem_produto else 'N/A'

        # Adicionar os dados a uma lista de dicionários
        ofertas.append({
            'nome_produto': nome_produto,
            'marca': marca,
            'preco_atual': preco_atual_texto,
            'preco_antigo': preco_antigo_texto,
            'loja_vendedora': loja_vendedora_texto,
            'oferta_do_dia': oferta_do_dia_texto,
            'frete_full': frete_full_texto,
            'porcentagem_desconto': porcentagem_desconto_texto,
            'imagem_produto_url': imagem_produto_url
        })

# Loop para fazer o scraping em cada página
for url in urls:
    scrape_page(url)

# Organizar os dados em tabela usando Pandas
df_ofertas = pd.DataFrame(ofertas)
```
# Ofertas Mercado Livre - Celulares

Este documento exibe as principais ofertas de celulares disponíveis no Mercado Livre, extraindo e apresentando informações relevantes, como nome do produto, marca, preço, loja vendedora, e mais.

[ofertas_mercado_livre.html](/documentos/ofertas_mercado_livre.html)

## Tabela de Ofertas

| **Nome do Produto**                                                                                          | **Marca**  | **Preço Atual** | **Preço Antigo** | **Loja Vendedora**   | **Oferta do Dia**     | **Frete Full**    | **Porcentagem de Desconto** | **Imagem do Produto**                                                                 |
|---------------------------------------------------------------------------------------------------------------|------------|-----------------|------------------|-----------------------|-----------------------|-------------------|----------------------------|---------------------------------------------------------------------------------------|
| Fone + Redmi 13c 256gb 8gb Ram Android 11 + Nota Fiscal                                                        | Outros     | 1.497           | 1.152            | N/A                   | SEM OFERTA DO DIA      | SEM FRETE FULL     | 23% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_873961-MLB78436044206_082024-W.jpg)         |
| Samsung Galaxy Z Flip 4 5g 128gb, 8gb Ram  Excelente                                                          | Samsung    | 2.499           | 2.074            | N/A                   | SEM OFERTA DO DIA      | SEM FRETE FULL     | 17% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_892393-MLB78624820171_082024-W.jpg)         |
| Celular Samsung Galaxy M35 5g , Câmera Tripla Até 50mp, Selfie 50mp, Tela Super Amoled + 6.6 120hz, 256gb, 8gb Ram - Azul Claro | Samsung    | 1.888           | 1.299            | Por Samsung           | SEM OFERTA DO DIA      | SEM FRETE FULL     | 31% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_882209-MLU76405989014_052024-W.jpg)         |
| Celular Samsung Galaxy M35 5g , Câmera Tripla Até 50mp, Selfie 50mp, Tela Super Amoled + 6.6 120hz, 256gb, 8gb Ram - Cinza     | Samsung    | 1.888           | 1.299            | Por Samsung           | SEM OFERTA DO DIA      | SEM FRETE FULL     | 31% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_882189-MLU76405954922_052024-W.jpg)         |
| Motorola Moto G85 5G 256GB Grafite 16GB RAM Boost                                                              | Motorola   | 2.299           | 1.709            | Por Motorola          | OFERTA DO DIA          | FRETE FULL         | 25% OFF no PIX             | [Imagem](https://http2.mlstatic.com/D_Q_NP_796103-MLU78093634301_072024-W.jpg)         |
| Motorola Moto G54 5G 256 GB Azul 8 GB RAM                                                                      | Motorola   | 1.899           | 1.149            | Por Mega Mamute       | SEM OFERTA DO DIA      | FRETE FULL         | 39% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_790094-MLU78394982184_082024-W.jpg)         |
| Motorola Moto G84 5G 256 GB Grafite 8 GB RAM                                                                   | Motorola   | 2.331           | 1.498            | Por Mega Mamute       | SEM OFERTA DO DIA      | FRETE FULL         | 35% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_902597-MLU77144155768_062024-W.jpg)         |
| Motorola Moto G85 5G 256GB Azul Vegan Leather 16GB RAM Boost                                                   | Motorola   | 2.299           | 1.709            | Por Motorola          | OFERTA DO DIA          | FRETE FULL         | 25% OFF no PIX             | [Imagem](https://http2.mlstatic.com/D_Q_NP_998515-MLU77871522886_072024-W.jpg)         |
| Motorola Edge 40 Neo 5G Dual SIM 256 GB Black beauty 8 GB RAM                                                  | Motorola   | 2.799           | 1.871            | Por Motorola          | SEM OFERTA DO DIA      | FRETE FULL         | 33% OFF no PIX             | [Imagem](https://http2.mlstatic.com/D_Q_NP_881477-MLA74650874883_022024-W.jpg)         |
| Realme C53 (50 Mpx) Dual SIM 128 GB mighty black 6 GB RAM                                                      | Realme     | 1.628           | 809              | Por Realme            | OFERTA DO DIA          | FRETE FULL         | 50% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_742601-MLU77356289551_062024-W.jpg)         |
| Samsung Galaxy A15 5G 5G 256 GB azul-escuro 8 GB RAM                                                           | Samsung    | 1.898           | 1.249            | Por Vikings           | OFERTA DO DIA          | FRETE FULL         | 34% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_657068-MLA76709066927_052024-W.jpg)         |
| Smartphone Realme Note 50 Dual Sim 128 Gb Champion 4 Gb Ram                                                    | Realme     | 838             | 779              | N/A                   | SEM OFERTA DO DIA      | SEM FRETE FULL     | 7% OFF                     | [Imagem](https://http2.mlstatic.com/D_Q_NP_816402-MLB75195716406_032024-W.jpg)         |
| Motorola Edge 40 Neo 5G Dual SIM 256 GB Caneel bay 8 GB RAM                                                    | Motorola   | 2.999           | 1.978            | N/A                   | SEM OFERTA DO DIA      | FRETE FULL         | 34% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_648373-MLA74525556610_022024-W.jpg)         |
| Motorola Moto G24 128GB Grafite 4GB RAM                                                                        | Motorola   | 1.208           | 744              | Por 123 Comprou       | OFERTA DO DIA          | FRETE FULL         | 38% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_919798-MLU74622536256_022024-W.jpg)         |
| Samsung Galaxy A35 5G 128GB Azul-escuro 6GB RAM                                                                | Samsung    | 2.281           | 1.394            | Por Vikings           | SEM OFERTA DO DIA      | FRETE FULL         | 38% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_808550-MLU75292967345_032024-W.jpg)         |
| Motorola Moto G04S 128GB Coral 4GB RAM                                                                         | Motorola   | 879             | 656              | Por Motorola          | OFERTA DO DIA          | SEM FRETE FULL     | 25% OFF                    | [Imagem](https://http2.mlstatic.com/D_Q_NP_822441-MLU75950846226_052024-W.jpg)         |


## Conexão ao PostgreSQL com Python

O código abaixo demonstra como conectar-se ao banco de dados **PostgreSQL**. Neste caso, o **Data Lake** é utilizado para armazenar os dados extraídos do Mercado Livre. O código usa a biblioteca **psycopg2** para realizar a conexão.

```diagram
PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2ZXJzaW9uPSIxLjEiIHdpZHRoPSI1MTFweCIgaGVpZ2h0PSIxMDFweCIgdmlld0JveD0iLTAuNSAtMC41IDUxMSAxMDEiIGNvbnRlbnQ9IiZsdDtteGZpbGUgaG9zdD0mcXVvdDtlbWJlZC5kaWFncmFtcy5uZXQmcXVvdDsgYWdlbnQ9JnF1b3Q7TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEyOC4wLjAuMCBTYWZhcmkvNTM3LjM2IEVkZy8xMjguMC4wLjAmcXVvdDsgdmVyc2lvbj0mcXVvdDsyNC43LjEyJnF1b3Q7Jmd0OyZsdDtkaWFncmFtIGlkPSZxdW90O3ExODFDZDVFZWtEd1lrdjdMRGhPJnF1b3Q7IG5hbWU9JnF1b3Q7UMOhZ2luYS0xJnF1b3Q7Jmd0OzFaWk5jNXN3RUlaL0RYZnhZY2UreG5YVGd6Tk54NGVlWmRpQ1VzRXlRaGpUWDkvRkVzaEVkdExPcEpucENmUnE5YkhQdmpzUXhKdnk5S0I0WFR4aUJqS0lXSFlLNGs5QkZFVnN1YVRIb1BSR1dVVUxJK1JLWkVZS25iQVh2OENLektxdHlLQ1pCV3BFcVVVOUYxT3NLa2oxVE9OS1lUY1ArNEZ5Zm1yTmMvQ0VmY3FscjM0WG1TN0dMTzZjL2dWRVhvd25oOHUxbVNuNUdHd3phUXFlWVhjaHhkc2czaWhFYmQ3SzB3YmtBRy9rWXRaOXZqRTdYVXhCcGY5a2dTMUVvL3N4Tjhnb1ZUdEVwUXZNc2VKeTY5UjdoVzJWd2JBQm81R0wyU0hXSklZa1BvUFd2YTBiYnpXU1ZPaFMybGx6NW5EUXpWdGJxY0ZXcFRZcXNvWG1LZ2NibFV6RXlHcUFKV2pWVTRnQ3liVTR6bmZudHViNUZEY3RmVUpCNTBaczlPZFlIT3ZPS0dIekxjd1Y3Q29IbDE0dXJ1R2tNL0xyK0cxT1J5NWJlOXRIVUNsWmdzU2RPQ3J3eXlNbHVYd29BMW1uSHNSVVlrdmIzWGVGMExDditabFhSNDEzRGZvUmxJYlQ2OWg5b0ZQanpyaUVhOXUxbmV1Qk1MWXh4WVgvMSt4MkNXYndYaUVWZTZTZXNORzVndjIzblFkcFF0TkxRVlpWOGR0NERzYlV1OE1rOFBSbmZyYjYxMWJUTm1EMXhyZzZYTHdQMDJUNUVpcnpvTjVkWVJxeWQ0QzYrbis2UC9HN1AvNG4zWjhzUHF6N0U5L1RQZUdzdktxb0FzdEQyM3hNa3ljdjhtZStJVmRYRExuNmV6L1MwSDNwREQvM3Z4QnZmd009Jmx0Oy9kaWFncmFtJmd0OyZsdDsvbXhmaWxlJmd0OyI+PGRlZnMvPjxnPjxnIGRhdGEtY2VsbC1pZD0iMCI+PGcgZGF0YS1jZWxsLWlkPSIxIj48ZyBkYXRhLWNlbGwtaWQ9IjYiPjxnPjxwYXRoIGQ9Ik0gMTMwIDUwIEwgMjEzLjYzIDUwIiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0ic3Ryb2tlIi8+PHBhdGggZD0iTSAyMTguODggNTAgTCAyMTEuODggNTMuNSBMIDIxMy42MyA1MCBMIDIxMS44OCA0Ni41IFoiIGZpbGw9InJnYigwLCAwLCAwKSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSIyIj48Zz48cGF0aCBkPSJNIDMyLjUgMjcuNSBDIDYuNSAyNy41IDAgNTAgMjAuOCA1NC41IEMgMCA2NC40IDIzLjQgODYgNDAuMyA3NyBDIDUyIDk1IDkxIDk1IDEwNCA3NyBDIDEzMCA3NyAxMzAgNTkgMTEzLjc1IDUwIEMgMTMwIDMyIDEwNCAxNCA4MS4yNSAyMyBDIDY1IDkuNSAzOSA5LjUgMzIuNSAyNy41IFoiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiIHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogMTI4cHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogNTBweDsgbWFyZ2luLWxlZnQ6IDFweDsiPjxkaXYgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiIHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiBIZWx2ZXRpY2E7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPk1lcmNhZG8gTGl2cmU8L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iNjUiIHk9IjU0IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+TWVyY2FkbyBMaXZyZTwvdGV4dD48L3N3aXRjaD48L2c+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjMiPjxnPjxwYXRoIGQ9Ik0gNDQwIDE1IEMgNDQwIDYuNzIgNDU1LjY3IDAgNDc1IDAgQyA0ODQuMjggMCA0OTMuMTggMS41OCA0OTkuNzUgNC4zOSBDIDUwNi4zMSA3LjIxIDUxMCAxMS4wMiA1MTAgMTUgTCA1MTAgODUgQyA1MTAgOTMuMjggNDk0LjMzIDEwMCA0NzUgMTAwIEMgNDU1LjY3IDEwMCA0NDAgOTMuMjggNDQwIDg1IFoiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjxwYXRoIGQ9Ik0gNTEwIDE1IEMgNTEwIDIzLjI4IDQ5NC4zMyAzMCA0NzUgMzAgQyA0NTUuNjcgMzAgNDQwIDIzLjI4IDQ0MCAxNSIgZmlsbD0ibm9uZSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiIHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogNjhweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiA2M3B4OyBtYXJnaW4tbGVmdDogNDQxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5Qb3N0Z3JlU1FMPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjQ3NSIgeT0iNjYiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5Qb3N0Z3JlU1FMPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iOCI+PGc+PHBhdGggZD0iTSAzMDAgNTAgTCA0MzMuNjMgNTAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDQzOC44OCA1MCBMIDQzMS44OCA1My41IEwgNDMzLjYzIDUwIEwgNDMxLjg4IDQ2LjUgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjQiPjxnPjxwYXRoIGQ9Ik0gMjYwIDEwIEwgMzAwIDUwIEwgMjYwIDkwIEwgMjIwIDUwIFoiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiIHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogNzhweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiA1MHB4OyBtYXJnaW4tbGVmdDogMjIxcHg7Ij48ZGl2IGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogSGVsdmV0aWNhOyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5QeXRob248L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iMjYwIiB5PSI1NCIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPlB5dGhvbjwvdGV4dD48L3N3aXRjaD48L2c+PC9nPjwvZz48L2c+PC9nPjwvZz48c3dpdGNoPjxnIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSIvPjxhIHRyYW5zZm9ybT0idHJhbnNsYXRlKDAsLTUpIiB4bGluazpocmVmPSJodHRwczovL3d3dy5kcmF3aW8uY29tL2RvYy9mYXEvc3ZnLWV4cG9ydC10ZXh0LXByb2JsZW1zIiB0YXJnZXQ9Il9ibGFuayI+PHRleHQgdGV4dC1hbmNob3I9Im1pZGRsZSIgZm9udC1zaXplPSIxMHB4IiB4PSI1MCUiIHk9IjEwMCUiPlRleHQgaXMgbm90IFNWRyAtIGNhbm5vdCBkaXNwbGF5PC90ZXh0PjwvYT48L3N3aXRjaD48L3N2Zz4=
```


```python
import psycopg2

# Configurações de conexão ao banco de dados PostgreSQL
db_config = {
    'host': '192.168.0.254',
    'port': 5432,
    'database': 'DataLake_OfertasML',
    'user': 'postgres',
    'password': '1234'
}

# Estabelecendo a conexão
try:
    connection = psycopg2.connect(**db_config)
    print("Conexão estabelecida com sucesso!")
except Exception as error:
    print(f"Erro ao conectar ao banco de dados: {error}")
finally:
    if 'connection' in locals() and connection:
        connection.close()
        print("Conexão encerrada.")
```
## Airflow: Trabalhando com DAG e Task

O **Airflow** é uma ferramenta de orquestração de pipelines em Python, que permite o agendamento de execuções em batch, facilitando a automação de tarefas diárias. Ele opera a partir de três componentes principais:

### 1. DAG (Directed Acyclic Graph)
- **Responsável pelo controle das tasks**, que são as tarefas diárias. 
- Dentro da DAG, são criadas as tasks, que representam as funções a serem executadas em sequência.

### 2. Task
- **As tasks são funções em Python** que executam os processos definidos no pipeline.
- Elas podem ser interligadas, formando dependências entre si, para garantir a ordem de execução.

### 3. Agendamento
- O Airflow trabalha com **processamento em batch**, ou seja, tarefas são executadas em horários agendados, de acordo com a configuração da DAG.


## Código em Python com Airflow

Abaixo está um exemplo do código utilizado neste projeto de ELT com o Mercado Livre e Airflow, onde os dados das ofertas de celulares são extraídos, transformados e carregados:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import psycopg2
from bs4 import BeautifulSoup
import pytz

# Configurações de conexão ao banco de dados PostgreSQL
db_config = {
    'host': '192.168.0.254',
    'port': 5432,
    'database': 'DataLake_OfertasML',
    'user': 'postgres',
    'password': '1234'
}

# Lista de URLs das páginas a serem coletadas
urls = [
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=2",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=3",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=4",
    "https://www.mercadolivre.com.br/ofertas?container_id=MLB779535-1&domain_id=MLB-CELLPHONES&page=5"
]

# Função para fazer o scraping
def scrape_data():
    ofertas = []
    def identificar_marca(nome_produto):
        marcas = ['Samsung', 'Motorola', 'Apple', 'Xiaomi', 'Realme', 'Multilaser']
        for marca in marcas:
            if marca.lower() in nome_produto.lower():
                return marca
        return 'Outros'

    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        produtos = soup.find_all('li', class_='promotion-item')

        for produto in produtos:
            titulo = produto.find('p', class_='promotion-item__title')
            nome_produto = titulo.text if titulo else 'N/A'
            marca = identificar_marca(nome_produto)
            preco_atual = produto.find('span', class_='andes-money-amount__fraction')
            preco_atual_texto = preco_atual.text if preco_atual else 'N/A'
            preco_antigo = produto.find_all('span', class_='andes-money-amount__fraction')
            preco_antigo_texto = preco_antigo[1].text if len(preco_antigo) > 1 else 'N/A'
            loja_vendedora = produto.find('span', class_='promotion-item__seller')
            loja_vendedora_texto = loja_vendedora.text if loja_vendedora else 'N/A'
            oferta_do_dia = produto.find('span', class_='promotion-item__today-offer-text')
            oferta_do_dia_texto = oferta_do_dia.text if oferta_do_dia else 'SEM OFERTA DO DIA'
            frete_full = produto.find('svg', class_='full-icon')
            frete_full_texto = 'FRETE FULL' if frete_full else 'SEM FRETE FULL'
            porcentagem_desconto = produto.find('span', class_='promotion-item__discount-text')
            porcentagem_desconto_texto = porcentagem_desconto.text if porcentagem_desconto else 'SEM DESCONTO'
            imagem_produto = produto.find('img', class_='promotion-item__img')
            imagem_produto_url = imagem_produto['src'] if imagem_produto else 'N/A'

            ofertas.append({
                'nome_produto': nome_produto,
                'marca': marca,
                'preco_atual': preco_atual_texto,
                'preco_antigo': preco_antigo_texto,
                'loja_vendedora': loja_vendedora_texto,
                'oferta_do_dia': oferta_do_dia_texto,
                'frete_full': frete_full_texto,
                'porcentagem_desconto': porcentagem_desconto_texto,
                'imagem_produto_url': imagem_produto_url
            })
    return ofertas

# Função para transformar os dados em tabela
def transformar_dados(ti):
    ofertas = ti.xcom_pull(task_ids='scrape_data')
    df_ofertas = pd.DataFrame(ofertas)
    ti.xcom_push(key='df_ofertas', value=df_ofertas)

# Função para inserir os dados no PostgreSQL
def inserir_dados(ti):
    df_ofertas = ti.xcom_pull(key='df_ofertas', task_ids='transformar_dados')
    conn = psycopg2.connect(**db_config)
    
    def criar_tabela_se_nao_existir(conn):
        query = """
        CREATE TABLE IF NOT EXISTS DL_OfertasCelularesML (
            id SERIAL PRIMARY KEY,
            nome_produto VARCHAR(255),
            marca VARCHAR(50),
            preco_atual VARCHAR(50),
            preco_antigo VARCHAR(50),
            loja_vendedora VARCHAR(255),
            oferta_do_dia VARCHAR(255),
            frete_full VARCHAR(50),
            porcentagem_desconto VARCHAR(50),
            imagem_produto_url TEXT,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def esvaziar_tabela(conn):
        query = "TRUNCATE TABLE DL_OfertasCelularesML;"
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

    def inserir_dados(conn, df):
        for _, row in df.iterrows():
            query = """
            INSERT INTO DL_OfertasCelularesML (
                nome_produto, marca, preco_atual, preco_antigo, loja_vendedora,
                oferta_do_dia, frete_full, porcentagem_desconto, imagem_produto_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            data = (
                row['nome_produto'], row['marca'], row['preco_atual'], row['preco_antigo'],
                row['loja_vendedora'], row['oferta_do_dia'], row['frete_full'],
                row['porcentagem_desconto'], row['imagem_produto_url']
            )
            with conn.cursor() as cur:
                cur.execute(query, data)
        conn.commit()

    try:
        criar_tabela_se_nao_existir(conn)
        esvaziar_tabela(conn)
        inserir_dados(conn, df_ofertas)
        print("Dados inseridos com sucesso!")
    finally:
        conn.close()

# Configurações da DAG
with DAG(
    'ELT_OfertasMercadoLivre',
    description='ELT das ofertas do mercado livre, agendado todo dia as 8:00 horas',
    schedule_interval='0 8 * * *',
    start_date=datetime(2023, 9, 8, tzinfo=pytz.timezone('America/Cuiaba')),
    catchup=False
) as dag:

    task_scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data
    )

    task_transformar_dados = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )

    task_inserir_dados = PythonOperator(
        task_id='inserir_dados',
        python_callable=inserir_dados
    )

    task_scrape_data >> task_transformar_dados >> task_inserir_dados
```
## DBT: Transformando Dados no Data Lake

O **dbt (Data Build Tool)** é uma ferramenta que facilita a transformação de dados em um Data Lake ou Data Warehouse utilizando SQL. Ele permite que analistas e engenheiros de dados escrevam consultas SQL para transformar os dados brutos em insights valiosos de forma eficiente. 
```diagram
PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2ZXJzaW9uPSIxLjEiIHdpZHRoPSI0ODFweCIgaGVpZ2h0PSIxNDFweCIgdmlld0JveD0iLTAuNSAtMC41IDQ4MSAxNDEiIGNvbnRlbnQ9IiZsdDtteGZpbGUgaG9zdD0mcXVvdDtlbWJlZC5kaWFncmFtcy5uZXQmcXVvdDsgYWdlbnQ9JnF1b3Q7TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NDsgcnY6MTMxLjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMTMxLjAmcXVvdDsgdmVyc2lvbj0mcXVvdDsyNC43LjEzJnF1b3Q7Jmd0OyZsdDtkaWFncmFtIGlkPSZxdW90OzJtSkF4VGtTUmhVTWt1YkZkNlNfJnF1b3Q7IG5hbWU9JnF1b3Q7UMOhZ2luYS0xJnF1b3Q7Jmd0OzFaVk5jNXN3RUlaL0RYZEEvc28xanBzZTNPbDAzSm1lRmRpQ0dzRXlZakdtdjc3Q1dzQlVUdEpPUFpucENlblZTc3MrZWlVRllsdWNIbzJzOGsrWWdnN2lNRDBGNGlHSTR6aGNMK3luVnpxbmJLS1ZFektqVWlkRmszQlFQNEhGa05WR3BWRFBBZ2xSazZybVlvSmxDUW5OTkdrTXR2T3c3NmpuV1N1WmdTY2NFcWw5OVp0S0tlY3E0dldrZndTVjVVUG1hSFhuUmdvNUJITWxkUzVUYkM4a3NRdkUxaUNTYXhXbkxlZ2Uzc0RGemZ2d3d1ajRZd1pLK3BNSnZCRTFkVU50a05wU3VZdUdjc3l3bEhvM3FmY0dtektGZm9IUTlxYVlQV0pseGNpS1A0Q280MzJURGFHVmNpbzBqN3FjZmFJWC81cWxHaHVUY0ZUTUd5MU5CaHdsUm1MV2FvQUZrT2xzaUFFdFNSM25xMHZlODJ5TW03RFlCcE81VG9sVEg2VnVlTkVIU2RJcWUva01Ia0c3cDFYZlREcXRMQ2dqYk1sdHJnZ09sVHdYMDlwVE1TZnk1SkR1bjBaQkpzL1pHZlRuaHV3eXdIcnRtRWJMRWVNUkRNSHBkWkErSXA0ZzJJWjhEdGZjYlNkVDM3R1VYL3A1RWY0NzA1WEg5UCt4b3ZDdHVId25Ld3JmaXZkZlBYSVRsK2h0ODkzQVIrUE4zQTAzbnUra0tMNWlwZFVObkxUMGtCeSs3SDB6YVcyZkIzZ2J4NXpkRGVBc2ZqdGxVZWpEMlZ4aHMvbDdOclk3dlIzbnNZc1hXT3grQVE9PSZsdDsvZGlhZ3JhbSZndDsmbHQ7L214ZmlsZSZndDsiPjxkZWZzLz48Zz48ZyBkYXRhLWNlbGwtaWQ9IjAiPjxnIGRhdGEtY2VsbC1pZD0iMSI+PGcgZGF0YS1jZWxsLWlkPSI0Ij48Zz48cGF0aCBkPSJNIDkwIDcwIEwgMTczLjYzIDcwIiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0ic3Ryb2tlIi8+PHBhdGggZD0iTSAxNzguODggNzAgTCAxNzEuODggNzMuNSBMIDE3My42MyA3MCBMIDE3MS44OCA2Ni41IFoiIGZpbGw9InJnYigwLCAwLCAwKSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSIyIj48Zz48cGF0aCBkPSJNIDAgMTUgQyAwIDYuNzIgMjAuMTUgMCA0NSAwIEMgNTYuOTMgMCA2OC4zOCAxLjU4IDc2LjgyIDQuMzkgQyA4NS4yNiA3LjIxIDkwIDExLjAyIDkwIDE1IEwgOTAgMTI1IEMgOTAgMTMzLjI4IDY5Ljg1IDE0MCA0NSAxNDAgQyAyMC4xNSAxNDAgMCAxMzMuMjggMCAxMjUgWiIgZmlsbD0icmdiKDI1NSwgMjU1LCAyNTUpIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PHBhdGggZD0iTSA5MCAxNSBDIDkwIDIzLjI4IDY5Ljg1IDMwIDQ1IDMwIEMgMjAuMTUgMzAgMCAyMy4yOCAwIDE1IiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3Qgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyIgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiA4OHB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDgzcHg7IG1hcmdpbi1sZWZ0OiAxcHg7Ij48ZGl2IHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyIgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7OyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5EYXRhIExha2U8L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iNDUiIHk9Ijg2IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+RGF0YSBMYWtlPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iNiI+PGc+PHBhdGggZD0iTSAzMDAgNzAgTCAzOTMuNjMgNzAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDM5OC44OCA3MCBMIDM5MS44OCA3My41IEwgMzkzLjYzIDcwIEwgMzkxLjg4IDY2LjUgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjMiPjxnPjxyZWN0IHg9IjE4MCIgeT0iNDAiIHdpZHRoPSIxMjAiIGhlaWdodD0iNjAiIHJ4PSI5IiByeT0iOSIgZmlsbD0icmdiKDI1NSwgMjU1LCAyNTUpIiBzdHJva2U9InJnYigwLCAwLCAwKSIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiIHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogMTE4cHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogNzBweDsgbWFyZ2luLWxlZnQ6IDE4MXB4OyI+PGRpdiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiIGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIj48ZGl2IHN0eWxlPSJkaXNwbGF5OiBpbmxpbmUtYmxvY2s7IGZvbnQtc2l6ZTogMTJweDsgZm9udC1mYW1pbHk6ICZxdW90O0hlbHZldGljYSZxdW90OzsgY29sb3I6IHJnYigwLCAwLCAwKTsgbGluZS1oZWlnaHQ6IDEuMjsgcG9pbnRlci1ldmVudHM6IGFsbDsgd2hpdGUtc3BhY2U6IG5vcm1hbDsgb3ZlcmZsb3ctd3JhcDogbm9ybWFsOyI+REJUPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjI0MCIgeT0iNzQiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5EQlQ8L3RleHQ+PC9zd2l0Y2g+PC9nPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSI1Ij48Zz48ZWxsaXBzZSBjeD0iNDQwIiBjeT0iNzAiIHJ4PSI0MCIgcnk9IjQwIiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3Qgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyIgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiA3OHB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDcwcHg7IG1hcmdpbi1sZWZ0OiA0MDFweDsiPjxkaXYgc3R5bGU9ImJveC1zaXppbmc6IGJvcmRlci1ib3g7IGZvbnQtc2l6ZTogMHB4OyB0ZXh0LWFsaWduOiBjZW50ZXI7IiBkYXRhLWRyYXdpby1jb2xvcnM9ImNvbG9yOiByZ2IoMCwgMCwgMCk7ICI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiAmcXVvdDtIZWx2ZXRpY2EmcXVvdDs7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPlNRTDwvZGl2PjwvZGl2PjwvZGl2PjwvZm9yZWlnbk9iamVjdD48dGV4dCB4PSI0NDAiIHk9Ijc0IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+U1FMPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjwvZz48L2c+PC9nPjxzd2l0Y2g+PGcgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ii8+PGEgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMCwtNSkiIHhsaW5rOmhyZWY9Imh0dHBzOi8vd3d3LmRyYXdpby5jb20vZG9jL2ZhcS9zdmctZXhwb3J0LXRleHQtcHJvYmxlbXMiIHRhcmdldD0iX2JsYW5rIj48dGV4dCB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmb250LXNpemU9IjEwcHgiIHg9IjUwJSIgeT0iMTAwJSI+VGV4dCBpcyBub3QgU1ZHIC0gY2Fubm90IGRpc3BsYXk8L3RleHQ+PC9hPjwvc3dpdGNoPjwvc3ZnPg==
```

### Principais Funcionalidades:

- **Transformação de Dados via SQL**: O dbt possibilita que todas as transformações sejam feitas exclusivamente com SQL, permitindo que você crie e gerencie pipelines de dados de maneira simplificada e eficiente.
  
- **Documentação Automatizada**: Uma das grandes vantagens do dbt é a capacidade de **documentar automaticamente** todas as transformações de dados. Cada modelo, coluna e tabela pode ser descrito, facilitando a criação de uma documentação clara e acessível para toda a equipe. Essa documentação pode ser visualizada via um servidor local ou publicada na web.

- **Versionamento de Código com Git**: O dbt se integra perfeitamente com sistemas de controle de versão, como o **Git**, permitindo que todas as mudanças no código e nas transformações sejam versionadas. Isso proporciona rastreamento das alterações e colaborações organizadas em equipes.

- **Testes Automatizados**: O dbt também permite a criação de testes automáticos em SQL, garantindo que a qualidade dos dados seja mantida ao longo do processo de transformação.

### Como o dbt funciona no Data Lake:

1. **Modelo SQL**: No dbt, as transformações de dados são definidas em arquivos `.sql`, que representam os modelos. Esses modelos contêm as consultas SQL que manipulam e transformam os dados.

2. **Arquitetura em Camadas**: O dbt incentiva a criação de uma arquitetura de dados em camadas. Isso significa que os dados brutos são primeiro carregados no Data Lake ou Data Warehouse e, em seguida, o dbt realiza as transformações necessárias para criar tabelas ou visualizações intermediárias e finais.

3. **Execução e Deploy**: Após definir os modelos, o dbt pode ser executado para processar as transformações e gerar os resultados no banco de dados. Isso é feito utilizando o comando `dbt run`, que executa todas as transformações em sequência.

Com o dbt, você pode garantir que todo o ciclo de vida dos dados, desde a transformação até a documentação e o versionamento, seja feito de forma transparente e colaborativa.

### Exemplos dos códigos

[views_produtos_fretefull_desconto.zip](/views_produtos_fretefull_desconto.zip)

```sql
{{
    config(
        materialized='view'
    )
}}

WITH cte AS (
  SELECT 
    nome_produto, 
    marca, 
    loja_vendedora AS loja,
    CAST(NULLIF(preco_atual, 'N/A') AS numeric) AS valor_novo,
    CAST(NULLIF(preco_antigo, 'N/A') AS numeric) AS valor_antigo,
    (CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) AS valor_desconto,
    ((CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) / CAST(NULLIF(preco_antigo, 'N/A') AS numeric)) * 100 AS porcentagem_ganho_desconto
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}
  WHERE 
    frete_full = 'FRETE FULL'
    AND preco_antigo IS NOT NULL 
    AND preco_atual IS NOT NULL
    AND (CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) > 0
)

SELECT * FROM cte
```

## Dbt Docs 
![screenshot_2.png](/screenshot_2.png)

### Fluxo
![screenshot_2.png](/screenshot_1.png)

# Metabase: Ferramenta Potente para Dashboards

O Metabase é uma ferramenta de análise de dados e criação de dashboards que facilita a visualização das informações provenientes de diversas fontes de dados. Sua integração com o Data Lake permite explorar e monitorar dados de forma eficiente e dinâmica.

## Conexão ao Data Lake e Criação de Dashboards

- O Metabase foi conectado ao **Data Lake**, permitindo o acesso direto às camadas de **views** do banco de dados.
- Com essa conexão, foram criados gráficos interativos e dinâmicos, que exibem as métricas e insights gerados pelas camadas de transformação de dados.
- Isso possibilitou a criação de dashboards completos para análise de dados, oferecendo uma visão consolidada e visual dos indicadores de desempenho.

```diagram
PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB2ZXJzaW9uPSIxLjEiIHdpZHRoPSI1OTFweCIgaGVpZ2h0PSIyOTJweCIgdmlld0JveD0iLTAuNSAtMC41IDU5MSAyOTIiIGNvbnRlbnQ9IiZsdDtteGZpbGUgaG9zdD0mcXVvdDtlbWJlZC5kaWFncmFtcy5uZXQmcXVvdDsgYWdlbnQ9JnF1b3Q7TW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NDsgcnY6MTMxLjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMTMxLjAmcXVvdDsgdmVyc2lvbj0mcXVvdDsyNC43LjE0JnF1b3Q7Jmd0OyZsdDtkaWFncmFtIGlkPSZxdW90O3MyRXd6R0tfZG5DOEN2ZGM2Nk5yJnF1b3Q7IG5hbWU9JnF1b3Q7UMOhZ2luYS0xJnF1b3Q7Jmd0OzVWaE5jNXN3RVAwMXZtWXNGTEI5ak8yMFBUalR6T1RROWlnakJkUUlsaEh5VjM5OVY1WUFBM2JxbVRMcFIwNldIcnVTOXIzZEZYaEVGOW4rbzJaRitnQmNxRkV3NXZzUlhZNkNJQmhQYnZISElnZUhURW5rZ0VSTDdpRFNBRS95aC9EZzJLTWJ5VVhaTWpRQXlzaWlEY2FRNXlJMkxZeHBEYnUyMlRPbzlxNEZTMFFQZUlxWjZxTmZKRGVwanlLWU5QZ25JWk8wMnBsRU0vY2tZNVd4ajZSTUdZZmRDVVR2UjNTaEFZd2JaZnVGVUphOGloZm45K0hDMC9wZ1d1VG1Hb2ZRT1d5WjJ2alk3bUlEMmgvT0hLcUk4WnlGSFc0eTVRem9mQ3Uwa2NqSmlxMkZlb1JTR2drNW1xekJHTWhPRE82VVRPd0RBd1dpcWNrVVRnZ09ZV09Vek1XaVZtbU1vRDhRK29yOXhhQklUUlhtbUlCTUdIMUFFKzhRaFo1ZG4xNmtZbnZYaUVVOWxKN29GSG1NK2ZSSTZwVWJCbkhnU1R4UDZHMlBPc0V4ZC93VXRFa2hnWnlwK3dhZGE5amtYSEJQUUdPekFzdllrYXJ2d3BpREx3UzJNZEFtRW5uUmg2L2Uvemo1WmljM1lUVmQ3azhmTGc4dHJ1MEJYMmNhNDRHTmpyMVY0Q3VPNlVSNEszcGVEeTBVTTNMYlh2MGN1OTcxRVNUdVcrdElwMjBkZ3pCc0wrR080TDA2R3RYSHVFcTIyWitRYlRENnA3OUovN1hKSGZTNnhaSVpoc2lLdllpTFBTTStZSlZ6b1NtR3ZFdWxFVThGT3dhenc5dWh6Y2phVWJwYTF3Q0xYNUlqMFo5ZHMvQjQ2VGdsNFRBZGcwU2RUQnYzTzhiMFRNY2daSUNXUVVpUHVyODIrV2cvK2NqNFBMV0RaeDhKL2pHZUJtK0lFWG16aGtoN3BmNGdESnV6c2wvbnY2aHBWaGJ1ZG4rV2V5dkVFUFZLWjkxNm5keUV2WXFkaGYyS25ZV1hTYjgyRWFNZU4vL2hTMVA5YnZnR0wwMlQ5MEFvcFcvM0ZqcDkvYUxHRDZQTTdtQkhMR1BjRGpqdWh4NVM0TWRSbDNja3dYU0syck1aSXhsQ242RTVrNXk3Qml6d3JtYnI0MUtXMmNJMnAyTm80WHdVTHUxYTJIT3IrOXhlN1ViREM4cWhyTDdMSE95ZFAzK1dTbldnQVNUcWRCRnlScUg2UysxVUlqckVyVC91YVRRS0ltVjU1bktMdzhRT202YnJOWVBTYVpXdWdXbGVWaTY0MjRuWHV4R1EzZ1pWMzM5RlF6SWJSa09jTmwvbTdrNXQvdCtnOXo4QiZsdDsvZGlhZ3JhbSZndDsmbHQ7L214ZmlsZSZndDsiPjxkZWZzLz48Zz48ZyBkYXRhLWNlbGwtaWQ9IjAiPjxnIGRhdGEtY2VsbC1pZD0iMSI+PGcgZGF0YS1jZWxsLWlkPSI1Ij48Zz48ZWxsaXBzZSBjeD0iNTc1IiBjeT0iNy41IiByeD0iNy41IiByeT0iNy41IiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PHBhdGggZD0iTSA1NzUgMTUgTCA1NzUgNDAgTSA1NzUgMjAgTCA1NjAgMjAgTSA1NzUgMjAgTCA1OTAgMjAgTSA1NzUgNDAgTCA1NjAgNjAgTSA1NzUgNDAgTCA1OTAgNjAiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7IiBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBmbGV4LXN0YXJ0OyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiAxcHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogNjdweDsgbWFyZ2luLWxlZnQ6IDU3NXB4OyI+PGRpdiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiIGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIj48ZGl2IHN0eWxlPSJkaXNwbGF5OiBpbmxpbmUtYmxvY2s7IGZvbnQtc2l6ZTogMTJweDsgZm9udC1mYW1pbHk6ICZxdW90O0hlbHZldGljYSZxdW90OzsgY29sb3I6IHJnYigwLCAwLCAwKTsgbGluZS1oZWlnaHQ6IDEuMjsgcG9pbnRlci1ldmVudHM6IGFsbDsgd2hpdGUtc3BhY2U6IG5vd3JhcDsiPkFjdG9yPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjU3NSIgeT0iNzkiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5BY3RvcjwvdGV4dD48L3N3aXRjaD48L2c+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjQiPjxnPjxwYXRoIGQ9Ik0gMTUwIDEzNSBMIDI5My42MyAxMzUiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDI5OC44OCAxMzUgTCAyOTEuODggMTM4LjUgTCAyOTMuNjMgMTM1IEwgMjkxLjg4IDEzMS41IFoiIGZpbGw9InJnYigwLCAwLCAwKSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSI5Ij48Zz48cGF0aCBkPSJNIDExMCA4MCBMIDExMCA0Ni4zNyIgZmlsbD0ibm9uZSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9InN0cm9rZSIvPjxwYXRoIGQ9Ik0gMTEwIDQxLjEyIEwgMTEzLjUgNDguMTIgTCAxMTAgNDYuMzcgTCAxMDYuNSA0OC4xMiBaIiBmaWxsPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iMiI+PGc+PHBhdGggZD0iTSA3MCA5NSBDIDcwIDg2LjcyIDg3LjkxIDgwIDExMCA4MCBDIDEyMC42MSA4MCAxMzAuNzggODEuNTggMTM4LjI4IDg0LjM5IEMgMTQ1Ljc5IDg3LjIxIDE1MCA5MS4wMiAxNTAgOTUgTCAxNTAgMTc1IEMgMTUwIDE4My4yOCAxMzIuMDkgMTkwIDExMCAxOTAgQyA4Ny45MSAxOTAgNzAgMTgzLjI4IDcwIDE3NSBaIiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48cGF0aCBkPSJNIDE1MCA5NSBDIDE1MCAxMDMuMjggMTMyLjA5IDExMCAxMTAgMTEwIEMgODcuOTEgMTEwIDcwIDEwMy4yOCA3MCA5NSIgZmlsbD0ibm9uZSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZS1taXRlcmxpbWl0PSIxMCIgcG9pbnRlci1ldmVudHM9ImFsbCIvPjwvZz48Zz48ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgtMC41IC0wLjUpIj48c3dpdGNoPjxmb3JlaWduT2JqZWN0IHN0eWxlPSJvdmVyZmxvdzogdmlzaWJsZTsgdGV4dC1hbGlnbjogbGVmdDsiIHBvaW50ZXItZXZlbnRzPSJub25lIiB3aWR0aD0iMTAwJSIgaGVpZ2h0PSIxMDAlIiByZXF1aXJlZEZlYXR1cmVzPSJodHRwOi8vd3d3LnczLm9yZy9UUi9TVkcxMS9mZWF0dXJlI0V4dGVuc2liaWxpdHkiPjxkaXYgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkveGh0bWwiIHN0eWxlPSJkaXNwbGF5OiBmbGV4OyBhbGlnbi1pdGVtczogdW5zYWZlIGNlbnRlcjsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogNzhweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiAxNDhweDsgbWFyZ2luLWxlZnQ6IDcxcHg7Ij48ZGl2IHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyIgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7OyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm9ybWFsOyBvdmVyZmxvdy13cmFwOiBub3JtYWw7Ij5EYXRhIExha2U8L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iMTEwIiB5PSIxNTEiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5EYXRhIExha2U8L3RleHQ+PC9zd2l0Y2g+PC9nPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSIxMSI+PGc+PHBhdGggZD0iTSAzNDcuNSA4Ny41IEwgMzQ3LjUgNDYuMzciIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJzdHJva2UiLz48cGF0aCBkPSJNIDM0Ny41IDQxLjEyIEwgMzUxIDQ4LjEyIEwgMzQ3LjUgNDYuMzcgTCAzNDQgNDguMTIgWiIgZmlsbD0icmdiKDAsIDAsIDApIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjEyIj48Zz48cGF0aCBkPSJNIDM5NSAxMzUgTCA0NTcuNSAxMzUgTCA1MTMuNjMgMTM1IiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0ic3Ryb2tlIi8+PHBhdGggZD0iTSA1MTguODggMTM1IEwgNTExLjg4IDEzOC41IEwgNTEzLjYzIDEzNSBMIDUxMS44OCAxMzEuNSBaIiBmaWxsPSJyZ2IoMCwgMCwgMCkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iMyI+PGc+PHJlY3QgeD0iMzAwIiB5PSI4Ny41IiB3aWR0aD0iOTUiIGhlaWdodD0iOTUiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7IiBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBjZW50ZXI7IGp1c3RpZnktY29udGVudDogdW5zYWZlIGNlbnRlcjsgd2lkdGg6IDkzcHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogMTM1cHg7IG1hcmdpbi1sZWZ0OiAzMDFweDsiPjxkaXYgc3R5bGU9ImJveC1zaXppbmc6IGJvcmRlci1ib3g7IGZvbnQtc2l6ZTogMHB4OyB0ZXh0LWFsaWduOiBjZW50ZXI7IiBkYXRhLWRyYXdpby1jb2xvcnM9ImNvbG9yOiByZ2IoMCwgMCwgMCk7ICI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiAmcXVvdDtIZWx2ZXRpY2EmcXVvdDs7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3JtYWw7IG92ZXJmbG93LXdyYXA6IG5vcm1hbDsiPk1ldGFCYXNlPC9kaXY+PC9kaXY+PC9kaXY+PC9mb3JlaWduT2JqZWN0Pjx0ZXh0IHg9IjM0OCIgeT0iMTM5IiBmaWxsPSJyZ2IoMCwgMCwgMCkiIGZvbnQtZmFtaWx5PSImcXVvdDtIZWx2ZXRpY2EmcXVvdDsiIGZvbnQtc2l6ZT0iMTJweCIgdGV4dC1hbmNob3I9Im1pZGRsZSI+TWV0YUJhc2U8L3RleHQ+PC9zd2l0Y2g+PC9nPjwvZz48L2c+PGcgZGF0YS1jZWxsLWlkPSI2Ij48Zz48ZWxsaXBzZSBjeD0iNTc1IiBjeT0iMTEyLjUiIHJ4PSI3LjUiIHJ5PSI3LjUiIGZpbGw9InJnYigyNTUsIDI1NSwgMjU1KSIgc3Ryb2tlPSJyZ2IoMCwgMCwgMCkiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48cGF0aCBkPSJNIDU3NSAxMjAgTCA1NzUgMTQ1IE0gNTc1IDEyNSBMIDU2MCAxMjUgTSA1NzUgMTI1IEwgNTkwIDEyNSBNIDU3NSAxNDUgTCA1NjAgMTY1IE0gNTc1IDE0NSBMIDU5MCAxNjUiIGZpbGw9Im5vbmUiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBzdHJva2UtbWl0ZXJsaW1pdD0iMTAiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7IiBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBmbGV4LXN0YXJ0OyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiAxcHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogMTcycHg7IG1hcmdpbi1sZWZ0OiA1NzVweDsiPjxkaXYgc3R5bGU9ImJveC1zaXppbmc6IGJvcmRlci1ib3g7IGZvbnQtc2l6ZTogMHB4OyB0ZXh0LWFsaWduOiBjZW50ZXI7IiBkYXRhLWRyYXdpby1jb2xvcnM9ImNvbG9yOiByZ2IoMCwgMCwgMCk7ICI+PGRpdiBzdHlsZT0iZGlzcGxheTogaW5saW5lLWJsb2NrOyBmb250LXNpemU6IDEycHg7IGZvbnQtZmFtaWx5OiAmcXVvdDtIZWx2ZXRpY2EmcXVvdDs7IGNvbG9yOiByZ2IoMCwgMCwgMCk7IGxpbmUtaGVpZ2h0OiAxLjI7IHBvaW50ZXItZXZlbnRzOiBhbGw7IHdoaXRlLXNwYWNlOiBub3dyYXA7Ij5BY3RvcjwvZGl2PjwvZGl2PjwvZGl2PjwvZm9yZWlnbk9iamVjdD48dGV4dCB4PSI1NzUiIHk9IjE4NCIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPkFjdG9yPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iNyI+PGc+PGVsbGlwc2UgY3g9IjU3NSIgY3k9IjIxNy41IiByeD0iNy41IiByeT0iNy41IiBmaWxsPSJyZ2IoMjU1LCAyNTUsIDI1NSkiIHN0cm9rZT0icmdiKDAsIDAsIDApIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PHBhdGggZD0iTSA1NzUgMjI1IEwgNTc1IDI1MCBNIDU3NSAyMzAgTCA1NjAgMjMwIE0gNTc1IDIzMCBMIDU5MCAyMzAgTSA1NzUgMjUwIEwgNTYwIDI3MCBNIDU3NSAyNTAgTCA1OTAgMjcwIiBmaWxsPSJub25lIiBzdHJva2U9InJnYigwLCAwLCAwKSIgc3Ryb2tlLW1pdGVybGltaXQ9IjEwIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3Qgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyIgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgZmxleC1zdGFydDsganVzdGlmeS1jb250ZW50OiB1bnNhZmUgY2VudGVyOyB3aWR0aDogMXB4OyBoZWlnaHQ6IDFweDsgcGFkZGluZy10b3A6IDI3N3B4OyBtYXJnaW4tbGVmdDogNTc1cHg7Ij48ZGl2IHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyIgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7OyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm93cmFwOyI+QWN0b3I8L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iNTc1IiB5PSIyODkiIGZpbGw9InJnYigwLCAwLCAwKSIgZm9udC1mYW1pbHk9IiZxdW90O0hlbHZldGljYSZxdW90OyIgZm9udC1zaXplPSIxMnB4IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIj5BY3RvcjwvdGV4dD48L3N3aXRjaD48L2c+PC9nPjwvZz48ZyBkYXRhLWNlbGwtaWQ9IjgiPjxnPjxyZWN0IHg9IjAiIHk9IjEwIiB3aWR0aD0iMjIwIiBoZWlnaHQ9IjMwIiBmaWxsPSJub25lIiBzdHJva2U9Im5vbmUiIHBvaW50ZXItZXZlbnRzPSJhbGwiLz48L2c+PGc+PGcgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoLTAuNSAtMC41KSI+PHN3aXRjaD48Zm9yZWlnbk9iamVjdCBzdHlsZT0ib3ZlcmZsb3c6IHZpc2libGU7IHRleHQtYWxpZ246IGxlZnQ7IiBwb2ludGVyLWV2ZW50cz0ibm9uZSIgd2lkdGg9IjEwMCUiIGhlaWdodD0iMTAwJSIgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ij48ZGl2IHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hodG1sIiBzdHlsZT0iZGlzcGxheTogZmxleDsgYWxpZ24taXRlbXM6IHVuc2FmZSBjZW50ZXI7IGp1c3RpZnktY29udGVudDogdW5zYWZlIGNlbnRlcjsgd2lkdGg6IDFweDsgaGVpZ2h0OiAxcHg7IHBhZGRpbmctdG9wOiAyNXB4OyBtYXJnaW4tbGVmdDogMTEwcHg7Ij48ZGl2IHN0eWxlPSJib3gtc2l6aW5nOiBib3JkZXItYm94OyBmb250LXNpemU6IDBweDsgdGV4dC1hbGlnbjogY2VudGVyOyIgZGF0YS1kcmF3aW8tY29sb3JzPSJjb2xvcjogcmdiKDAsIDAsIDApOyAiPjxkaXYgc3R5bGU9ImRpc3BsYXk6IGlubGluZS1ibG9jazsgZm9udC1zaXplOiAxMnB4OyBmb250LWZhbWlseTogJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7OyBjb2xvcjogcmdiKDAsIDAsIDApOyBsaW5lLWhlaWdodDogMS4yOyBwb2ludGVyLWV2ZW50czogYWxsOyB3aGl0ZS1zcGFjZTogbm93cmFwOyI+RGF0YSBMYWtlIGNvbSBhIGNhbWFkYSBkYXMgdmlld3M8L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iMTEwIiB5PSIyOSIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPkRhdGEgTGFrZSBjb20gYSBjYW1hZGEgZGFzIHZpZXdzPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjxnIGRhdGEtY2VsbC1pZD0iMTAiPjxnPjxyZWN0IHg9IjI1Mi41IiB5PSIxMCIgd2lkdGg9IjE5MCIgaGVpZ2h0PSIzMCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSJub25lIiBwb2ludGVyLWV2ZW50cz0iYWxsIi8+PC9nPjxnPjxnIHRyYW5zZm9ybT0idHJhbnNsYXRlKC0wLjUgLTAuNSkiPjxzd2l0Y2g+PGZvcmVpZ25PYmplY3Qgc3R5bGU9Im92ZXJmbG93OiB2aXNpYmxlOyB0ZXh0LWFsaWduOiBsZWZ0OyIgcG9pbnRlci1ldmVudHM9Im5vbmUiIHdpZHRoPSIxMDAlIiBoZWlnaHQ9IjEwMCUiIHJlcXVpcmVkRmVhdHVyZXM9Imh0dHA6Ly93d3cudzMub3JnL1RSL1NWRzExL2ZlYXR1cmUjRXh0ZW5zaWJpbGl0eSI+PGRpdiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94aHRtbCIgc3R5bGU9ImRpc3BsYXk6IGZsZXg7IGFsaWduLWl0ZW1zOiB1bnNhZmUgY2VudGVyOyBqdXN0aWZ5LWNvbnRlbnQ6IHVuc2FmZSBjZW50ZXI7IHdpZHRoOiAxcHg7IGhlaWdodDogMXB4OyBwYWRkaW5nLXRvcDogMjVweDsgbWFyZ2luLWxlZnQ6IDM0OHB4OyI+PGRpdiBzdHlsZT0iYm94LXNpemluZzogYm9yZGVyLWJveDsgZm9udC1zaXplOiAwcHg7IHRleHQtYWxpZ246IGNlbnRlcjsiIGRhdGEtZHJhd2lvLWNvbG9ycz0iY29sb3I6IHJnYigwLCAwLCAwKTsgIj48ZGl2IHN0eWxlPSJkaXNwbGF5OiBpbmxpbmUtYmxvY2s7IGZvbnQtc2l6ZTogMTJweDsgZm9udC1mYW1pbHk6ICZxdW90O0hlbHZldGljYSZxdW90OzsgY29sb3I6IHJnYigwLCAwLCAwKTsgbGluZS1oZWlnaHQ6IDEuMjsgcG9pbnRlci1ldmVudHM6IGFsbDsgd2hpdGUtc3BhY2U6IG5vd3JhcDsiPjxkaXY+TWV0YUJhc2UgY29tIG9zIGRhc2hib2FyZHM8L2Rpdj48L2Rpdj48L2Rpdj48L2Rpdj48L2ZvcmVpZ25PYmplY3Q+PHRleHQgeD0iMzQ4IiB5PSIyOSIgZmlsbD0icmdiKDAsIDAsIDApIiBmb250LWZhbWlseT0iJnF1b3Q7SGVsdmV0aWNhJnF1b3Q7IiBmb250LXNpemU9IjEycHgiIHRleHQtYW5jaG9yPSJtaWRkbGUiPk1ldGFCYXNlIGNvbSBvcyBkYXNoYm9hcmRzPC90ZXh0Pjwvc3dpdGNoPjwvZz48L2c+PC9nPjwvZz48L2c+PC9nPjxzd2l0Y2g+PGcgcmVxdWlyZWRGZWF0dXJlcz0iaHR0cDovL3d3dy53My5vcmcvVFIvU1ZHMTEvZmVhdHVyZSNFeHRlbnNpYmlsaXR5Ii8+PGEgdHJhbnNmb3JtPSJ0cmFuc2xhdGUoMCwtNSkiIHhsaW5rOmhyZWY9Imh0dHBzOi8vd3d3LmRyYXdpby5jb20vZG9jL2ZhcS9zdmctZXhwb3J0LXRleHQtcHJvYmxlbXMiIHRhcmdldD0iX2JsYW5rIj48dGV4dCB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmb250LXNpemU9IjEwcHgiIHg9IjUwJSIgeT0iMTAwJSI+VGV4dCBpcyBub3QgU1ZHIC0gY2Fubm90IGRpc3BsYXk8L3RleHQ+PC9hPjwvc3dpdGNoPjwvc3ZnPg==
```

![screenshot_3.png](/screenshot_3.png)

![screenshot_4.png](/screenshot_4.png)
## Benefícios

- **Interface amigável**: O Metabase permite criar dashboards rapidamente, sem a necessidade de codificação avançada.
- **Análises visuais**: Através dos gráficos e painéis, é possível explorar tendências e padrões nos dados de forma intuitiva.
- **Integração com o Data Lake**: A conexão direta com o banco de dados permite o uso em tempo real das informações disponíveis nas **views**.

Com o Metabase, a análise de dados se torna mais acessível e visual, permitindo uma tomada de decisão mais informada e baseada em dados.

# Conclusão

A integração de **Airflow**, **DBT**, **PostgreSQL** e outras ferramentas como **Python** e **Metabase** demonstrou-se uma solução robusta e eficiente para automatizar processos de ELT (Extração, Carga e Transformação de dados). Cada ferramenta desempenhou um papel crucial na construção de uma pipeline dinâmica e escalável, permitindo o tratamento e a análise de grandes volumes de dados com eficiência.

Ao final do processo, conseguimos extrair informações valiosas das ofertas de celulares no Mercado Livre, transformá-las em dados utilizáveis e visualizá-las em dashboards interativos, tudo de maneira automatizada e agendada.

### Benefícios do Projeto:

1. **Automação Total**: Utilizando o Airflow para orquestrar as tarefas, toda a pipeline de dados foi automatizada, eliminando a necessidade de intervenção manual.
2. **Transformações Otimizadas com DBT**: A organização e transformação dos dados em camadas facilitaram a geração de insights consistentes e confiáveis.
3. **Armazenamento Seguro e Escalável**: Com o PostgreSQL, os dados foram armazenados de forma segura e com alta performance para grandes volumes.
4. **Visualização de Dados**: A integração com o Metabase proporcionou uma visualização eficiente, possibilitando a criação de dashboards dinâmicos e insights visuais.
5. **Flexibilidade e Escalabilidade**: A arquitetura escolhida é facilmente escalável, podendo ser adaptada a outros projetos de dados com diferentes fontes e volumes de dados.

### Próximos Passos:

Este projeto pode ser expandido para incluir novas fontes de dados, diferentes tipos de ofertas ou produtos, além da implementação de novas funcionalidades, como previsões de vendas e análises de tendências. A adição de mais testes e validações automatizadas com DBT também pode garantir a integridade contínua dos dados ao longo do tempo.

Dessa forma, esta solução proporciona um excelente ponto de partida para a criação de pipelines de dados automatizados e inteligentes, viabilizando a tomada de decisões baseada em dados de maneira ágil e eficiente.





