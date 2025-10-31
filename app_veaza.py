import datetime as dt
from typing import Tuple, Dict
from bson import ObjectId
import streamlit as st
import pymongo
import pandas as pd

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Veaza Plada — Omnicanal", page_icon="🛒", layout="wide")

MONGODB_URI = st.secrets["app"]["MONGODB_URI"]
if not MONGODB_URI:
    st.error("❌ Falta MONGODB_URI en st.secrets['app']['MONGODB_URI']")
    st.stop()

client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=8000, connectTimeoutMS=8000)
client.admin.command("ping")

db = client["veaza_plada_db"]

# colecciones
canales      = db["canales"]
ubicaciones  = db["ubicaciones"]
categorias   = db["categorias"]
productos    = db["productos"]
inventario   = db["inventario"]
clientes     = db["clientes"]
carritos     = db["carritos"]
ordenes      = db["ordenes"]
pagos        = db["pagos"]
envios       = db["envios"]
promociones  = db["promociones"]
devoluciones = db["devoluciones"]
eventos      = db["eventos"]

# para soft-delete
SOFT_FILTER = {"deleted": {"$ne": True}}

# =========================================================
# UTILIDADES
# =========================================================
def _date_input_to_dt(d):
    if not d:
        return None
    return dt.datetime(d.year, d.month, d.day)

def _safe_float(x):
    if x in (None, "", " "):
        return None
    return float(x)

def _require(ok: bool, msg: str):
    if not ok:
        st.error("❌ " + msg)
        st.stop()

@st.cache_data(ttl=60)
def get_catalogos():
    _cats  = list(categorias.find(SOFT_FILTER, {"_id":1, "nombre":1, "slug":1}).sort("nombre",1))
    _prods = list(productos.find(SOFT_FILTER, {"_id":1, "sku":1, "nombre":1, "precio":1, "moneda":1}).sort("nombre",1))
    _clis  = list(clientes.find(SOFT_FILTER, {"_id":1, "doc_tipo":1, "doc_num":1, "nombres":1, "apellidos":1}).sort([("apellidos",1),("nombres",1)]))
    _ubis  = list(ubicaciones.find({}, {"_id":1, "nombre":1, "ciudad":1, "tipo_ubicacion":1}).sort("nombre",1))
    _cans  = list(canales.find({}, {"_id":1, "codigo":1, "nombre":1, "tipo":1}).sort("codigo",1))

    cat_map = {str(c["_id"]): f'{c["nombre"]} ({c.get("slug","")})' for c in _cats}
    prod_map = {str(p["_id"]): f'{p["nombre"]} — {p.get("sku","")}' for p in _prods}
    cli_map = {str(c["_id"]): f'{c.get("apellidos","")}, {c.get("nombres","")} — {c["doc_tipo"]}-{c["doc_num"]}' for c in _clis}
    ubi_map = {str(u["_id"]): f'{u["nombre"]} ({u["tipo_ubicacion"]})' for u in _ubis}
    can_map = {str(c["_id"]): f'{c["codigo"]} — {c["nombre"]} ({c["tipo"]})' for c in _cans}

    precio_by_id = {str(p["_id"]): float(p.get("precio", 0)) for p in _prods}

    return (
        _cats, _prods, _clis, _ubis, _cans,
        cat_map, prod_map, cli_map, ubi_map, can_map,
        precio_by_id
    )

def _opts(first_label: str, mapping: Dict[str,str]):
    """Construye diccionario label->id para selects."""
    opts = {first_label: None}
    for k, v in mapping.items():
        opts[v] = k
    return opts

# =========================================================
# VALIDADORES
# =========================================================
def validar_producto(d: dict) -> Tuple[bool, str]:
    for f in ["sku", "nombre", "precio", "moneda", "estado"]:
        if d.get(f) in (None, "", []):
            return False, f"'{f}' es obligatorio."
    if d["estado"] not in ["ACTIVO", "INACTIVO", "DESCONTINUADO"]:
        return False, "estado inválido."
    try:
        if float(d["precio"]) < 0:
            return False, "precio no puede ser negativo."
    except Exception:
        return False, "precio debe ser numérico."
    if len(d["moneda"]) != 3:
        return False, "moneda debe ser ISO de 3 letras (PEN, USD...)."
    return True, ""

def validar_cliente(d: dict) -> tuple[bool, str]:
    obligatorios = ["doc_tipo", "doc_num", "nombres", "apellidos"]
    for f in obligatorios:
        if not d.get(f):
            return False, f"'{f}' es obligatorio."
    if d["doc_tipo"] not in ["DNI", "CE", "PAS"]:
        return False, "doc_tipo inválido."
    # el resto puede ser None
    return True, ""

def validar_orden(d: dict) -> Tuple[bool, str]:
    for f in ["codigo", "cliente_id", "canal_codigo", "moneda", "items"]:
        if not d.get(f):
            return False, f"'{f}' es obligatorio."
    if len(d["items"]) == 0:
        return False, "la orden debe tener al menos 1 ítem."
    return True, ""

def validar_pago(d: dict) -> Tuple[bool, str]:
    for f in ["orden_id", "monto", "moneda", "metodo", "estado"]:
        if not d.get(f) and d.get(f) != 0:
            return False, f"'{f}' es obligatorio."
    return True, ""

# =========================================================
# TABS
# =========================================================
tabs = st.tabs([
    "📦 Productos",
    "👤 Clientes",
    "🧾 Órdenes",
    "💳 Pagos",
    "📊 Inventario",
    "🛒 Carritos",
    "🚚 Envíos",
    "🏷️ Promos",
    "📜 Eventos"
])

(
    CATS, PRODS, CLIS, UBIS, CANS,
    cat_map, prod_map, cli_map, ubi_map, can_map,
    precio_by_id
) = get_catalogos()

cat_opts  = _opts("— Selecciona —", cat_map)
cli_opts  = _opts("— Selecciona —", cli_map)
prod_opts = _opts("— Selecciona —", prod_map)
ubi_opts  = _opts("— Selecciona —", ubi_map)

# =========================================================
# 1. PRODUCTOS
# =========================================================
with tabs[0]:
    st.subheader("📦 Productos")

    colf1, colf2, colf3 = st.columns([2, 1, 1])
    with colf1:
        prod_txt = st.text_input("Buscar (sku, nombre, desc.)", "", key="p_buscar")
    with colf2:
        prod_estado = st.selectbox("Estado", ["— Todos —", "ACTIVO", "INACTIVO", "DESCONTINUADO"], key="p_estado_f")
    with colf3:
        prod_cat = st.selectbox("Categoría", ["— Todos —"] + list(cat_map.values()), key="p_cat_f")

    filt = dict(SOFT_FILTER)
    if prod_txt:
        filt["$or"] = [
            {"sku": {"$regex": prod_txt, "$options": "i"}},
            {"nombre": {"$regex": prod_txt, "$options": "i"}},
            {"descripcion": {"$regex": prod_txt, "$options": "i"}},
        ]
    if prod_estado != "— Todos —":
        filt["estado"] = prod_estado
    if prod_cat != "— Todos —":
        # buscar id por label
        prod_cat_id = [k for k,v in cat_map.items() if v == prod_cat][0]
        filt["categoria_id"] = ObjectId(prod_cat_id)

    productos_rows = list(productos.find(filt).sort("nombre", 1))
    df_prod = pd.DataFrame([
        {
            "ID": str(r["_id"]),
            "SKU": r.get("sku",""),
            "Nombre": r.get("nombre",""),
            "Categoría": cat_map.get(str(r.get("categoria_id")), ""),
            "Precio": r.get("precio",""),
            "Moneda": r.get("moneda",""),
            "Estado": r.get("estado",""),
        }
        for r in productos_rows
    ])
    st.dataframe(df_prod, use_container_width=True, hide_index=True)

    st.markdown("### ➕ Crear producto")
    with st.form("prod_create", clear_on_submit=True):
        sku = st.text_input("SKU", key="prod_sku")
        nombre = st.text_input("Nombre", key="prod_nombre")
        desc = st.text_area("Descripción", "", key="prod_desc")
        cat_lbl = st.selectbox("Categoría", list(cat_opts.keys()), key="prod_cat_new")
        cat_val = cat_opts[cat_lbl]
        precio = st.text_input("Precio", key="prod_precio")
        moneda = st.text_input("Moneda (ISO 3)", value="PEN", key="prod_moneda")
        estado = st.selectbox("Estado", ["ACTIVO","INACTIVO","DESCONTINUADO"], key="prod_estado")
        submit_p = st.form_submit_button("Crear producto", use_container_width=True)
        if submit_p:
            doc = {
                "sku": sku.strip(),
                "nombre": nombre.strip(),
                "descripcion": desc.strip() or None,
                "categoria_id": ObjectId(cat_val) if cat_val else None,
                "precio": _safe_float(precio),
                "moneda": moneda.strip().upper(),
                "estado": estado,
                "imagenes": [],
                "deleted": False
            }
            ok, msg = validar_producto(doc)
            if not ok:
                st.error("❌ " + msg)
            else:
                try:
                    productos.insert_one(doc)
                    st.success("✅ Producto creado.")
                    st.cache_data.clear()
                    st.rerun()
                except pymongo.errors.DuplicateKeyError:
                    st.error("❌ SKU ya existe (revisa índice único o soft-delete).")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    st.markdown("### ✏️ Editar / 🗑️ Eliminar")
    # seleccionar de la lista ya filtrada
    prod_labels = ["— Selecciona —"] + [f'{r["nombre"]} — {r["sku"]}' for r in productos_rows]
    sel_prod = st.selectbox("Producto", prod_labels, key="prod_sel_edit")
    if sel_prod != "— Selecciona —":
        prod_row = productos_rows[prod_labels.index(sel_prod)-1]
        with st.form("prod_edit"):
            e_nombre = st.text_input("Nombre", value=prod_row.get("nombre",""), key="prod_edit_nombre")
            e_desc = st.text_area("Descripción", value=prod_row.get("descripcion","") or "", key="prod_edit_desc")
            e_cat_lbl = st.selectbox("Categoría", list(cat_opts.keys()),
                                     index=list(cat_opts.keys()).index(cat_map.get(str(prod_row.get("categoria_id")), "— Selecciona —")) if cat_map.get(str(prod_row.get("categoria_id"))) else 0,
                                     key="prod_edit_cat")
            e_cat_val = cat_opts[e_cat_lbl]
            e_precio = st.text_input("Precio", value=str(prod_row.get("precio","") or ""), key="prod_edit_precio")
            e_moneda = st.text_input("Moneda (ISO 3)", value=prod_row.get("moneda","PEN"), key="prod_edit_moneda")
            e_estado = st.selectbox("Estado", ["ACTIVO","INACTIVO","DESCONTINUADO"],
                                    index=["ACTIVO","INACTIVO","DESCONTINUADO"].index(prod_row.get("estado","ACTIVO")),
                                    key="prod_edit_estado")
            colu1, colu2 = st.columns(2)
            with colu1:
                save_p = st.form_submit_button("💾 Guardar cambios", use_container_width=True)
            with colu2:
                del_p = st.form_submit_button("🗑️ Eliminar", use_container_width=True)
            if save_p:
                upd = {
                    "nombre": e_nombre.strip(),
                    "descripcion": e_desc.strip() or None,
                    "categoria_id": ObjectId(e_cat_val) if e_cat_val else None,
                    "precio": _safe_float(e_precio),
                    "moneda": e_moneda.strip().upper(),
                    "estado": e_estado,
                }
                ok, msg = validar_producto({**prod_row, **upd})
                if not ok:
                    st.error("❌ " + msg)
                else:
                    productos.update_one({"_id": prod_row["_id"]}, {"$set": upd})
                    st.success("✅ Cambios guardados.")
                    st.cache_data.clear()
                    st.rerun()
            if del_p:
                productos.update_one({"_id": prod_row["_id"]}, {"$set": {"deleted": True, "deleted_at": dt.datetime.utcnow()}})
                st.success("✅ Producto eliminado lógicamente.")
                st.cache_data.clear()
                st.rerun()

# =========================================================
# 2. CLIENTES
# =========================================================
with tabs[1]:
    st.subheader("👤 Clientes")
    cli_txt = st.text_input("Buscar (nombres, apellidos, doc, correo)", "", key="c_buscar")
    cf = dict(SOFT_FILTER)
    if cli_txt:
        cf["$or"] = [
            {"nombres": {"$regex": cli_txt, "$options": "i"}},
            {"apellidos": {"$regex": cli_txt, "$options": "i"}},
            {"doc_num": {"$regex": cli_txt, "$options": "i"}},
            {"correo": {"$regex": cli_txt, "$options": "i"}},
        ]
    cli_rows = list(clientes.find(cf).sort([("apellidos",1), ("nombres",1)]))
    df_cli = pd.DataFrame([
        {
            "ID": str(r["_id"]),
            "Doc": f'{r.get("doc_tipo")}-{r.get("doc_num")}',
            "Nombres": r.get("nombres",""),
            "Apellidos": r.get("apellidos",""),
            "Correo": r.get("correo",""),
            "Teléfono": r.get("telefono",""),
            "Segmento": r.get("segmento",""),
        }
        for r in cli_rows
    ])
    st.dataframe(df_cli, use_container_width=True, hide_index=True)

    st.markdown("### ➕ Crear cliente")
    with st.form("cli_create", clear_on_submit=True):
        tdoc = st.selectbox("Tipo doc.", ["DNI","CE","PAS"], key="cli_tdoc")
        dnum = st.text_input("N° documento", key="cli_dnum")
        nom  = st.text_input("Nombres", key="cli_nom")
        ape  = st.text_input("Apellidos", key="cli_ape")
        cor  = st.text_input("Correo", key="cli_cor")
        tel  = st.text_input("Teléfono", key="cli_tel")
        dirc = st.text_input("Dirección", key="cli_dir")
        segm = st.selectbox("Segmento", ["REGULAR","VIP","NUEVO"], key="cli_seg")
        sc = st.form_submit_button("Crear cliente", use_container_width=True)
        if sc:
            doc = {
                "doc_tipo": tdoc,
                "doc_num": dnum.strip(),
                "nombres": nom.strip(),
                "apellidos": ape.strip(),
                "correo": cor.strip() or None,
                "telefono": tel.strip() or None,
                "direccion": dirc.strip() or None,
                "segmento": segm,
                "creado_en": dt.datetime.utcnow(),
                "deleted": False
            }
            ok, msg = validar_cliente(doc)
            if not ok:
                st.error("❌ " + msg)
            else:
                try:
                    clientes.insert_one(doc)
                    st.success("✅ Cliente creado.")
                    st.cache_data.clear()
                    st.rerun()
                except pymongo.errors.DuplicateKeyError:
                    st.error("❌ Ya existe cliente con ese documento.")
                except Exception as e:
                    st.error(f"❌ Error: {e}")

    st.markdown("### ✏️ Editar / 🗑️ Eliminar")
    cli_labels = ["— Selecciona —"] + [f'{r["apellidos"]}, {r["nombres"]} — {r["doc_tipo"]}-{r["doc_num"]}' for r in cli_rows]
    sel_cli = st.selectbox("Cliente", cli_labels, key="cli_sel_edit")
    if sel_cli != "— Selecciona —":
        cli_row = cli_rows[cli_labels.index(sel_cli)-1]
        with st.form("cli_edit"):
            e_tdoc = st.selectbox("Tipo doc.", ["DNI","CE","PAS"], index=["DNI","CE","PAS"].index(cli_row.get("doc_tipo","DNI")), key="cli_e_tdoc")
            e_dnum = st.text_input("N° documento", value=cli_row.get("doc_num",""), key="cli_e_dnum")
            e_nom  = st.text_input("Nombres", value=cli_row.get("nombres",""), key="cli_e_nom")
            e_ape  = st.text_input("Apellidos", value=cli_row.get("apellidos",""), key="cli_e_ape")
            e_cor  = st.text_input("Correo", value=cli_row.get("correo","") or "", key="cli_e_cor")
            e_tel  = st.text_input("Teléfono", value=cli_row.get("telefono","") or "", key="cli_e_tel")
            e_dir  = st.text_input("Dirección", value=cli_row.get("direccion","") or "", key="cli_e_dir")
            e_seg  = st.selectbox("Segmento", ["REGULAR","VIP","NUEVO"],
                                  index=["REGULAR","VIP","NUEVO"].index(cli_row.get("segmento","REGULAR")),
                                  key="cli_e_seg")
            colc1, colc2 = st.columns(2)
            with colc1:
                cu = st.form_submit_button("💾 Guardar cambios", use_container_width=True)
            with colc2:
                cd = st.form_submit_button("🗑️ Eliminar", use_container_width=True)

            if cu:
                upd = {
                    "doc_tipo": e_tdoc,
                    "doc_num": e_dnum.strip(),
                    "nombres": e_nom.strip(),
                    "apellidos": e_ape.strip(),
                    "correo": e_cor.strip() or None,
                    "telefono": e_tel.strip() or None,
                    "direccion": e_dir.strip() or None,
                    "segmento": e_seg,
                }
                ok, msg = validar_cliente({**cli_row, **upd})
                if not ok:
                    st.error("❌ " + msg)
                else:
                    clientes.update_one({"_id": cli_row["_id"]}, {"$set": upd})
                    st.success("✅ Cambios guardados.")
                    st.cache_data.clear()
                    st.rerun()
            if cd:
                clientes.update_one({"_id": cli_row["_id"]}, {"$set": {"deleted": True, "deleted_at": dt.datetime.utcnow()}})
                st.success("✅ Cliente eliminado lógicamente.")
                st.cache_data.clear()
                st.rerun()

# =========================================================
# 3. ÓRDENES
# =========================================================
with tabs[2]:
    st.subheader("🧾 Órdenes")
    o_txt = st.text_input("Buscar por código", "", key="o_buscar")
    o_estado = st.selectbox(
        "Estado",
        ["— Todos —","CREADA","PAGADA","PREPARACION","EN_RUTA","LISTA_RECOJO","ENTREGADA","CANCELADA","DEVUELTA"],
        key="o_estado"
    )
    fo = {}
    if o_txt:
        fo["codigo"] = {"$regex": o_txt, "$options": "i"}
    if o_estado != "— Todos —":
        fo["estado"] = o_estado
    order_rows = list(ordenes.find(fo).sort("creada_en", -1))
    df_ord = pd.DataFrame([
        {
            "Código": r.get("codigo",""),
            "Cliente": cli_map.get(str(r.get("cliente_id")), str(r.get("cliente_id"))),
            "Canal": r.get("canal_codigo",""),
            "Estado": r.get("estado",""),
            "Total": r.get("total",""),
            "Moneda": r.get("moneda",""),
            "Creada": r.get("creada_en").isoformat() if r.get("creada_en") else "",
        }
        for r in order_rows
    ])
    st.dataframe(df_ord, use_container_width=True, hide_index=True)
    total_general = sum(r.get("total", 0) or 0 for r in order_rows)
    st.caption(f"🧮 Total de órdenes listadas: {round(total_general, 2)}")

    st.markdown("### ➕ Crear orden")
    oc_lbl = st.selectbox("Cliente", list(cli_opts.keys()), key="o_cli")
    oc_val = cli_opts[oc_lbl]
    oc_canal = st.selectbox("Canal", ["WEB","APP","TIENDA","DELIVERY","PICKUP"], key="o_canal")
    oc_moneda = st.text_input("Moneda (ISO 3)", value="PEN", key="o_moneda")
    n_items = st.number_input("N° de ítems", min_value=1, max_value=10, value=1, key="o_nitems")
    o_items = []
    for i in range(int(n_items)):
        p_lbl = st.selectbox(f"Producto #{i+1}", list(prod_opts.keys()), key=f"o_prod_{i}")
        p_val = prod_opts[p_lbl]
        qty   = st.number_input(f"Cantidad #{i+1}", min_value=1, value=1, key=f"o_qty_{i}")
        price = float(precio_by_id.get(p_val, 0.0)) if p_val else 0.0
        subtotal_sugerido = round(price * qty, 2)
        st.caption(f"Precio unit.: {price} | Subtotal sugerido: {subtotal_sugerido}")
        st.caption(f"Precio sugerido: {price}")
        o_items.append({"producto_id": p_val, "cantidad": int(qty), "precio": price})

    if st.button("Crear orden", key="o_create"):
        _require(oc_val, "Debes seleccionar un cliente.")
        if any(x["producto_id"] is None for x in o_items):
            st.error("❌ Todos los ítems deben tener producto.")
        else:
            for x in o_items:
                x["producto_id"] = ObjectId(x["producto_id"])
                x["subtotal"] = round(x["precio"] * x["cantidad"], 2)
            total = round(sum(x["subtotal"] for x in o_items), 2)
            codigo = f"ORD-{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            doc = {
                "codigo": codigo,
                "cliente_id": ObjectId(oc_val),
                "canal_codigo": oc_canal,
                "estado": "CREADA",
                "items": o_items,
                "moneda": oc_moneda.strip().upper(),
                "total": total,
                "creada_en": dt.datetime.utcnow(),
                "actualizada_en": dt.datetime.utcnow()
            }
            ok, msg = validar_orden(doc)
            if not ok:
                st.error("❌ " + msg)
            else:
                ordenes.insert_one(doc)
                st.success(f"✅ Orden {codigo} creada (total {total} {doc['moneda']}).")
                st.cache_data.clear()
                st.rerun()

# =========================================================
# 4. PAGOS
# =========================================================
with tabs[3]:
    st.subheader("💳 Pagos")

    ord_input = st.text_input("Código de orden", "", key="pago_orden_code").strip()

    current_order = None

    # helper para saber si parece un ObjectId
    def es_objectid(s: str) -> bool:
        if len(s) != 24:
            return False
        try:
            int(s, 16)
            return True
        except ValueError:
            return False

    # 1) intentar encontrar la orden
    if ord_input:
        if es_objectid(ord_input):
            # buscar por _id
            from bson import ObjectId
            current_order = ordenes.find_one({"_id": ObjectId(ord_input)})
        else:
            # buscar por codigo
            current_order = ordenes.find_one({"codigo": ord_input})

    # 2) si la encontré, muestro el form
    if current_order:
        st.info(
            f"Orden: **{current_order['codigo']}** "
            f"({current_order['_id']}) — Total: {current_order.get('total')} {current_order.get('moneda')}"
        )

        with st.form("pago_form", clear_on_submit=True):
            monto = st.text_input("Monto", value=str(current_order.get("total","") or ""), key="pago_monto")
            moneda = st.text_input("Moneda (ISO 3)", value=current_order.get("moneda","PEN"), key="pago_moneda")
            metodo = st.selectbox("Método", ["TARJETA","YAPE","PLIN","TRANSFERENCIA","EFECTIVO"], key="pago_metodo")
            estado = st.selectbox("Estado", ["PENDIENTE","APROBADO","RECHAZADO","REEMBOLSADO"], key="pago_estado")
            if st.form_submit_button("Registrar pago", use_container_width=True):
                # evitar duplicado aprobado mismo monto
                pago_dup = pagos.find_one({
                    "orden_id": current_order["_id"],
                    "monto": float(monto),
                    "estado": "APROBADO"
                })
                if pago_dup and estado == "APROBADO":
                    st.error("❌ Ya existe un pago APROBADO con ese monto para esta orden.")
                else:
                    pago_doc = {
                        "orden_id": current_order["_id"],
                        "monto": float(monto),
                        "moneda": moneda.strip().upper(),
                        "metodo": metodo,
                        "estado": estado,
                        "transaccion_ref": f"TRX-{current_order['codigo']}",
                        "creado_en": dt.datetime.utcnow(),
                    }
                    pagos.insert_one(pago_doc)

                    # si se aprobó, actualizar la orden
                    if estado == "APROBADO":
                        aprobados = list(pagos.find({"orden_id": current_order["_id"], "estado": "APROBADO"}))
                        total_pagado = round(sum(p["monto"] for p in aprobados), 2)
                        nuevo_estado = "PAGADA" if total_pagado >= current_order.get("total", 0) else current_order.get("estado","CREADA")
                        ordenes.update_one(
                            {"_id": current_order["_id"]},
                            {"$set": {
                                "total_pagado": total_pagado,
                                "estado": nuevo_estado,
                                "actualizada_en": dt.datetime.utcnow()
                            }}
                        )

                    st.success("✅ Pago registrado.")
                    st.cache_data.clear()
                    st.rerun()
    else:
        if ord_input:
            st.warning("⚠️ No se encontró una orden con ese código o _id.")

    # 3) tabla de pagos
    #    si hay orden -> solo sus pagos
    #    si no hay -> últimos 100
    if current_order:
        pagos_rows = list(
            pagos.find({"orden_id": current_order["_id"]})
                 .sort("creado_en", -1)
        )
        # mapeo directo
        order_code_map = {str(current_order["_id"]): current_order["codigo"]}
    else:
        pagos_rows = list(
            pagos.find({})
                 .sort("creado_en", -1)
                 .limit(100)
        )
        # para mostrar el código tenemos que buscar las órdenes de esos pagos
        orden_ids = list({p["orden_id"] for p in pagos_rows})
        orden_docs = list(ordenes.find({"_id": {"$in": list(orden_ids)}}))
        order_code_map = {str(o["_id"]): o.get("codigo","") for o in orden_docs}

    df_pagos = pd.DataFrame([
        {
            "Orden ID": str(r.get("orden_id")),
            "Código de orden": order_code_map.get(str(r.get("orden_id")), ""),
            "Monto": r.get("monto",""),
            "Moneda": r.get("moneda",""),
            "Método": r.get("metodo",""),
            "Estado": r.get("estado",""),
            "Ref": r.get("transaccion_ref",""),
            "Creado": r.get("creado_en").isoformat() if r.get("creado_en") else "",
        }
        for r in pagos_rows
    ])

    st.dataframe(df_pagos, use_container_width=True, hide_index=True)
# =========================================================
# 5. INVENTARIO
# =========================================================
with tabs[4]:
    st.subheader("📊 Inventario por ubicación")
    ip_lbl = st.selectbox("Producto", list(prod_opts.keys()), key="inv_prod")
    ip_val = prod_opts[ip_lbl]
    iu_lbl = st.selectbox("Ubicación", list(ubi_opts.keys()), key="inv_ubi")
    iu_val = ubi_opts[iu_lbl]

    inv_f = {}
    if ip_val:
        inv_f["producto_id"] = ObjectId(ip_val)
    inv_rows = list(inventario.find(inv_f))
    df_inv = pd.DataFrame([
        {
            "ID": str(r["_id"]),
            "Producto": prod_map.get(str(r["producto_id"]), str(r["producto_id"])),
            "Ubicación": ubi_map.get(str(r["ubicacion_id"]), str(r["ubicacion_id"])),
            "Stock": r.get("stock",0),
            "Reservado": r.get("reservado",0),
            "Seguridad": r.get("seguridad",0),
            "Actualizado": r.get("actualizado_en").isoformat() if r.get("actualizado_en") else "",
        }
        for r in inv_rows
    ])
    st.dataframe(df_inv, use_container_width=True, hide_index=True)

    st.markdown("### ➕ Upsert de inventario")
    with st.form("inv_form", clear_on_submit=True):
        stock = st.number_input("Stock", min_value=0, value=0, key="inv_stock")
        reservado = st.number_input("Reservado", min_value=0, value=0, key="inv_res")
        seg = st.number_input("Stock de seguridad", min_value=0, value=0, key="inv_seg")
        inv_submit = st.form_submit_button("Guardar inventario", use_container_width=True)
        if inv_submit:
            _require(ip_val, "Selecciona producto.")
            _require(iu_val, "Selecciona ubicación.")
            doc = {
                "producto_id": ObjectId(ip_val),
                "ubicacion_id": ObjectId(iu_val),
                "stock": int(stock),
                "reservado": int(reservado),
                "seguridad": int(seg),
                "actualizado_en": dt.datetime.utcnow()
            }
            inventario.update_one(
                {"producto_id": doc["producto_id"], "ubicacion_id": doc["ubicacion_id"]},
                {"$set": doc},
                upsert=True
            )
            st.success("✅ Inventario guardado.")
            st.cache_data.clear()
            st.rerun()

# =========================================================
# 6. CARRITOS
# =========================================================
with tabs[5]:
    st.subheader("🛒 Carritos")
    cc_lbl = st.selectbox("Cliente", list(cli_opts.keys()), key="cart_cli")
    cc_val = cli_opts[cc_lbl]
    canal = st.selectbox("Canal", ["WEB","APP","TIENDA","DELIVERY","PICKUP"], key="cart_canal")
    carrito_actual = carritos.find_one({"cliente_id": ObjectId(cc_val), "canal_codigo": canal}) if cc_val else None

    nci = st.number_input("N° ítems", min_value=1, max_value=10, value=1, key="cart_n")
    cart_items = []
    for i in range(int(nci)):
        p_lbl = st.selectbox(f"Producto #{i+1}", list(prod_opts.keys()), key=f"cart_p_{i}")
        p_val = prod_opts[p_lbl]
        qty = st.number_input(f"Cantidad #{i+1}", min_value=1, value=1, key=f"cart_q_{i}")
        price = float(precio_by_id.get(p_val, 0.0)) if p_val else 0.0
        subtotal_sugerido = round(price * qty, 2)
        st.caption(f"Precio unit.: {price} | Subtotal sugerido: {subtotal_sugerido}")
        st.caption(f"Precio sugerido: {price}")
        cart_items.append({
            "producto_id": p_val,
            "cantidad": int(qty),
            "precio_unitario": price,
            "moneda": "PEN"
        })

    if st.button("Guardar carrito", key="cart_save"):
        _require(cc_val, "Selecciona cliente.")
        if any(x["producto_id"] is None for x in cart_items):
            st.error("❌ Todos los ítems deben tener producto.")
        else:
            for x in cart_items:
                x["producto_id"] = ObjectId(x["producto_id"])
            doc = {
                "cliente_id": ObjectId(cc_val),
                "canal_codigo": canal,
                "items": cart_items,
                "actualizado_en": dt.datetime.utcnow()
            }
            carritos.update_one(
                {"cliente_id": doc["cliente_id"], "canal_codigo": doc["canal_codigo"]},
                {"$set": doc},
                upsert=True
            )
            st.success("✅ Carrito guardado.")
            st.cache_data.clear()
            st.rerun()

    if carrito_actual:
        st.markdown("**Carrito actual:**")
        car = dict(carrito_actual)
        car["_id"] = str(car["_id"])
        car["cliente_id"] = str(car["cliente_id"])
        for it in car.get("items", []):
            it["producto_id"] = str(it["producto_id"])
        st.json(car, expanded=False)

# =========================================================
# 7. ENVÍOS
# =========================================================
with tabs[6]:
    st.subheader("🚚 Envíos")
    ord_code_e = st.text_input("Código de orden", "", key="ship_code")
    ord_e = ordenes.find_one({"codigo": ord_code_e}) if ord_code_e else None
    if ord_e:
        with st.form("ship_form", clear_on_submit=True):
            tipo = st.selectbox("Tipo", ["DELIVERY","PICKUP"], key="ship_tipo")
            estado = st.selectbox("Estado", ["PENDIENTE","PREPARANDO","LISTO","EN_RUTA","ENTREGADO","CANCELADO"], key="ship_estado")
            proveedor = st.text_input("Proveedor", value="Veaza Logistics", key="ship_prov")
            tracking = st.text_input("Tracking", value=f"VL-{ord_e['codigo']}", key="ship_track")
            ssub = st.form_submit_button("Guardar envío", use_container_width=True)
            if ssub:
                doc = {
                    "orden_id": ord_e["_id"],
                    "tipo": tipo,
                    "estado": estado,
                    "proveedor": proveedor or None,
                    "tracking": tracking or None,
                    "actualizado_en": dt.datetime.utcnow()
                }
                envios.update_one({"orden_id": ord_e["_id"]}, {"$set": doc}, upsert=True)
                st.success("✅ Envío guardado.")
                st.cache_data.clear()
                st.rerun()

    env_rows = list(envios.find({}).sort("actualizado_en",-1).limit(100))
    df_env = pd.DataFrame([
        {
            "Orden": str(r.get("orden_id")),
            "Tipo": r.get("tipo",""),
            "Estado": r.get("estado",""),
            "Proveedor": r.get("proveedor",""),
            "Tracking": r.get("tracking",""),
            "Actualizado": r.get("actualizado_en").isoformat() if r.get("actualizado_en") else "",
        }
        for r in env_rows
    ])
    st.dataframe(df_env, use_container_width=True, hide_index=True)

# =========================================================
# 8. PROMOS
# =========================================================
with tabs[7]:
    st.subheader("🏷️ Promociones")
    txt = st.text_input("Buscar promo", "", key="promo_buscar")
    fp = {}
    if txt:
        fp["$or"] = [
            {"codigo": {"$regex": txt, "$options": "i"}},
            {"descripcion": {"$regex": txt, "$options": "i"}},
        ]
    promo_rows = list(promociones.find(fp).sort("codigo",1))
    df_prom = pd.DataFrame([
        {
            "Código": r.get("codigo",""),
            "Descripción": r.get("descripcion",""),
            "Tipo": r.get("tipo",""),
            "Valor": r.get("valor",""),
            "Activo": r.get("activo",""),
        }
        for r in promo_rows
    ])
    st.dataframe(df_prom, use_container_width=True, hide_index=True)

    with st.form("promo_form", clear_on_submit=True):
        pc = st.text_input("Código", key="promo_code")
        pdsc = st.text_input("Descripción", key="promo_desc")
        pt = st.selectbox("Tipo", ["PCT_DESC","MONTO_DESC","ENVIO_GRATIS"], key="promo_type")
        pv = st.text_input("Valor", "", key="promo_val")
        pact = st.checkbox("Activo", value=True, key="promo_act")
        sub_p = st.form_submit_button("Crear promo", use_container_width=True)
        if sub_p:
            doc = {
                "codigo": pc.strip(),
                "descripcion": pdsc.strip(),
                "tipo": pt,
                "valor": _safe_float(pv),
                "activo": pact
            }
            ok, msg = validar_pago(doc) if False else (True,"")  # dummy
            try:
                promociones.insert_one(doc)
                st.success("✅ Promo creada.")
                st.cache_data.clear()
                st.rerun()
            except pymongo.errors.DuplicateKeyError:
                st.error("❌ Código de promo duplicado.")
            except Exception as e:
                st.error(f"❌ Error: {e}")

# =========================================================
# 9. EVENTOS (solo lectura)
# =========================================================
with tabs[8]:
    st.subheader("📜 Eventos (auditoría)")
    rows = list(eventos.find({}).sort("timestamp",-1).limit(200))
    df_evt = pd.DataFrame([
        {
            "Tipo": r.get("tipo",""),
            "Entidad": r.get("entidad",""),
            "EntidadId": str(r.get("entidad_id")) if r.get("entidad_id") else "",
            "Fecha": r.get("timestamp").isoformat() if r.get("timestamp") else "",
            "Payload": r.get("payload","")
        }
        for r in rows
    ])
    st.dataframe(df_evt, use_container_width=True, hide_index=True)
