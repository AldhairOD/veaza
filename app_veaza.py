import datetime as dt
from typing import Tuple, Dict
from bson import ObjectId
import streamlit as st
import pymongo
import pandas as pd

# =======================
# CONFIGURACIÓN
# =======================
st.set_page_config(page_title="Veaza Plada — Omnicanal (MongoDB Atlas)", page_icon="🛒", layout="wide")

MONGODB_URI = st.secrets["app"]["MONGODB_URI"]
if not MONGODB_URI:
    st.error("❌ Falta MONGODB_URI en st.secrets['app']['MONGODB_URI']")
    st.stop()

client = pymongo.MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
client.admin.command("ping")
db = client["veaza_plada_db"]

# Colecciones
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

st.title("🛒 Veaza Plada — Ecosistema Omnicanal (MongoDB Atlas)")

# =======================
# UTILIDADES / CACHÉS
# =======================
def _date_input_to_dt(d):
    if not d: return None
    return dt.datetime(d.year, d.month, d.day)

@st.cache_data(ttl=60)
def get_catalogos():
    _cats  = list(categorias.find({}, {"_id":1,"nombre":1,"slug":1}).sort("nombre",1))
    _prods = list(productos.find({}, {"_id":1,"sku":1,"nombre":1,"precio":1,"moneda":1,"categoria_id":1}).sort("nombre",1))
    _clis  = list(clientes.find({}, {"_id":1,"doc_tipo":1,"doc_num":1,"nombres":1,"apellidos":1}).sort([("apellidos",1),("nombres",1)]))
    _ubis  = list(ubicaciones.find({}, {"_id":1,"nombre":1,"ciudad":1,"tipo_ubicacion":1}).sort("nombre",1))
    _cans  = list(canales.find({}, {"_id":1,"codigo":1,"nombre":1,"tipo":1}).sort("codigo",1))
    cat_map  = {str(c["_id"]): f'{c.get("nombre")} ({c.get("slug")})' for c in _cats}
    prod_map = {str(p["_id"]): f'{p.get("nombre")} — {p.get("sku")}' for p in _prods}
    cli_map  = {str(c["_id"]): f'{c.get("apellidos","")}, {c.get("nombres","")} — {c.get("doc_tipo")}-{c.get("doc_num")}' for c in _clis}
    ubi_map  = {str(u["_id"]): f'{u.get("nombre")} ({u.get("tipo_ubicacion")})' + (f' — {u.get("ciudad")}' if u.get("ciudad") else "") for u in _ubis}
    can_map  = {str(c["_id"]): f'{c.get("codigo")} — {c.get("nombre")} ({c.get("tipo")})' for c in _cans}
    sku_by_id = {str(p["_id"]): p.get("sku") for p in _prods}
    precio_by_id = {str(p["_id"]): float(p.get("precio",0)) for p in _prods}
    return (_cats,_prods,_clis,_ubis,_cans, cat_map,prod_map,cli_map,ubi_map,can_map, sku_by_id,precio_by_id)

def _opts(first_label: str, mapping: Dict[str,str]):
    opts = {first_label: None}
    opts.update({v:k for k,v in mapping.items()})
    return opts

def _safe_float(x):
    if x in (None,""): return None
    return float(x)

def _require(ok: bool, msg: str):
    if not ok:
        st.error(f"❌ {msg}")
        st.stop()

# Validadores
def validar_producto(d: dict) -> Tuple[bool,str]:
    req = ["sku","nombre","precio","moneda","estado"]
    for f in req:
        if d.get(f) in (None,"",[]):
            return False, f"'{f}' es obligatorio."
    if d["estado"] not in ["ACTIVO","INACTIVO","DESCONTINUADO"]:
        return False, "estado inválido."
    try:
        if float(d["precio"]) < 0: return False, "precio no puede ser negativo."
    except Exception:
        return False, "precio debe ser numérico."
    if len(d.get("moneda","")) != 3:
        return False, "moneda debe ser ISO de 3 letras (PEN, USD, ...)."
    return True, ""

def validar_cliente(d: dict) -> Tuple[bool,str]:
    for f in ["doc_tipo","doc_num","nombres","apellidos"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["doc_tipo"] not in ["DNI","CE","PAS"]:
        return False, "doc_tipo inválido (DNI, CE, PAS)."
    return True, ""

def validar_canal(d: dict) -> Tuple[bool,str]:
    for f in ["codigo","nombre","tipo"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["tipo"] not in ["TIENDA","WEB","APP","DELIVERY","PICKUP"]:
        return False, "tipo inválido."
    return True, ""

def validar_ubicacion(d: dict) -> Tuple[bool,str]:
    for f in ["nombre","tipo_ubicacion","pais"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["tipo_ubicacion"] not in ["TIENDA_FISICA","ALMACEN","DARK_STORE"]:
        return False, "tipo_ubicacion inválido."
    return True, ""

def validar_categoria(d: dict) -> Tuple[bool,str]:
    for f in ["nombre","slug"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    return True, ""

def validar_inventario(d: dict) -> Tuple[bool,str]:
    for f in ["producto_id","ubicacion_id","stock","seguridad"]:
        if d.get(f) in (None,""): return False, f"'{f}' es obligatorio."
    if int(d["stock"]) < 0 or int(d["seguridad"]) < 0:
        return False, "stock/seguridad no pueden ser negativos."
    return True, ""

def validar_carrito(d: dict) -> Tuple[bool,str]:
    if not d.get("cliente_id"): return False,"cliente_id es obligatorio."
    if not d.get("canal_codigo"): return False,"canal_codigo es obligatorio."
    if not d.get("items") or len(d["items"])==0: return False,"items no puede estar vacío."
    return True, ""

def validar_orden(d: dict) -> Tuple[bool,str]:
    for f in ["codigo","cliente_id","canal_codigo","moneda","items","total"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d.get("estado") not in ["CREADA","PAGADA","PREPARACION","EN_RUTA","LISTA_RECOJO","ENTREGADA","CANCELADA","DEVUELTA"]:
        return False, "estado inválido."
    return True, ""

def validar_pago(d: dict) -> Tuple[bool,str]:
    for f in ["orden_id","monto","moneda","metodo","estado"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["estado"] not in ["PENDIENTE","APROBADO","RECHAZADO","REEMBOLSADO"]:
        return False,"estado inválido."
    return True,""

def validar_envio(d: dict) -> Tuple[bool,str]:
    for f in ["orden_id","tipo","estado"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["tipo"] not in ["DELIVERY","PICKUP"]:
        return False,"tipo inválido."
    if d["estado"] not in ["PENDIENTE","PREPARANDO","LISTO","EN_RUTA","ENTREGADO","CANCELADO"]:
        return False,"estado inválido."
    return True,""

def validar_promo(d: dict) -> Tuple[bool,str]:
    for f in ["codigo","descripcion","tipo","activo"]:
        if d.get(f) in (None,""): return False, f"'{f}' es obligatorio."
    if d["tipo"] not in ["PCT_DESC","MONTO_DESC","ENVIO_GRATIS"]:
        return False,"tipo inválido."
    return True,""

def validar_devolucion(d: dict) -> Tuple[bool,str]:
    for f in ["orden_id","estado","items"]:
        if not d.get(f): return False, f"'{f}' es obligatorio."
    if d["estado"] not in ["SOLICITADA","APROBADA","RECHAZADA","RECIBIDA","REEMBOLSADA","CERRADA"]:
        return False,"estado inválido."
    return True,""

# =======================
# TABS
# =======================
tabs = st.tabs([
    "📡 Canales","🏬 Ubicaciones","🏷️ Categorías","📦 Productos","📊 Inventario",
    "👤 Clientes","🛒 Carritos","🧾 Órdenes","💳 Pagos","🚚 Envíos",
    "🏷️ Promos","↩️ Devoluciones","📜 Eventos"
])

(CATS,PRODS,CLIS,UBIS,CANS, cat_map,prod_map,cli_map,ubi_map,can_map, sku_by_id,precio_by_id) = get_catalogos()
cat_opts  = _opts("— Selecciona —", cat_map)
cli_opts  = _opts("— Selecciona —", cli_map)
prod_opts = _opts("— Selecciona —", prod_map)
ubi_opts  = _opts("— Selecciona —", ubi_map)

# -----------------------
# 📡 CANALES
# -----------------------
with tabs[0]:
    st.subheader("📡 Canales")
    texto = st.text_input("Buscar por código/nombre", "", key="can_buscar")
    filtro = {"$or":[{"codigo":{"$regex":texto,"$options":"i"}},{"nombre":{"$regex":texto,"$options":"i"}}]} if texto else {}
    rows = list(canales.find(filtro).sort("codigo",1))
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_id"] = df["_id"].astype(str)
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("can_create", clear_on_submit=True):
        c_codigo = st.text_input("Código (ej. WEB)", key="can_codigo")
        c_nombre = st.text_input("Nombre", key="can_nombre")
        c_tipo   = st.selectbox("Tipo", ["TIENDA","WEB","APP","DELIVERY","PICKUP"], key="can_tipo")
        c_activo = st.checkbox("Activo", value=True, key="can_activo")
        if st.form_submit_button("Crear canal", use_container_width=True):
            doc = {"codigo":c_codigo.strip(),"nombre":c_nombre.strip(),"tipo":c_tipo,"activo":bool(c_activo)}
            ok,msg = validar_canal(doc)
            if ok:
                try:
                    canales.insert_one(doc)
                    st.success("✅ Canal creado.")
                    st.cache_data.clear()
                except pymongo.errors.DuplicateKeyError:
                    st.error("❌ Canal duplicado (índice único 'codigo').")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
            else:
                st.error(msg)

# -----------------------
# 🏬 UBICACIONES
# -----------------------
with tabs[1]:
    st.subheader("🏬 Ubicaciones")
    txt = st.text_input("Buscar por nombre/ciudad", "", key="ubi_buscar")
    filtro = {"$or":[{"nombre":{"$regex":txt,"$options":"i"}},{"ciudad":{"$regex":txt,"$options":"i"}}]} if txt else {}
    rows = list(ubicaciones.find(filtro).sort("nombre",1))
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_id"] = df["_id"].astype(str)
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("ubi_create", clear_on_submit=True):
        u_nombre = st.text_input("Nombre", key="ubi_nombre")
        u_tipo   = st.selectbox("Tipo ubicación", ["TIENDA_FISICA","ALMACEN","DARK_STORE"], key="ubi_tipo")
        u_pais   = st.text_input("País", value="Perú", key="ubi_pais")
        u_ciudad = st.text_input("Ciudad", value="", key="ubi_ciudad")
        u_dir    = st.text_input("Dirección", value="", key="ubi_dir")
        u_activo = st.checkbox("Activo", value=True, key="ubi_activo")
        if st.form_submit_button("Crear ubicación", use_container_width=True):
            doc = {"nombre":u_nombre.strip(),"tipo_ubicacion":u_tipo,"pais":u_pais.strip(),
                   "ciudad":u_ciudad.strip() or None,"direccion":u_dir.strip() or None,"geo":None,"activo":bool(u_activo)}
            ok,msg = validar_ubicacion(doc)
            if ok:
                try:
                    ubicaciones.insert_one(doc)
                    st.success("✅ Ubicación creada.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
            else:
                st.error(msg)

# -----------------------
# 🏷️ CATEGORÍAS
# -----------------------
with tabs[2]:
    st.subheader("🏷️ Categorías")
    txt = st.text_input("Buscar por nombre/slug", "", key="cat_buscar")
    f = {"$or":[{"nombre":{"$regex":txt,"$options":"i"}},{"slug":{"$regex":txt,"$options":"i"}}]} if txt else {}
    rows = list(categorias.find(f).sort("nombre",1))
    df = pd.DataFrame(rows)
    if not df.empty:
        df["_id"] = df["_id"].astype(str)
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("cat_create", clear_on_submit=True):
        c_nombre = st.text_input("Nombre", key="cat_nombre")
        c_slug   = st.text_input("Slug", key="cat_slug")
        c_parent = st.selectbox("Parent", list(cat_opts.keys()), key="cat_parent")
        c_parent_val = cat_opts[c_parent]
        if st.form_submit_button("Crear categoría", use_container_width=True):
            doc = {"nombre":c_nombre.strip(),"slug":c_slug.strip(),"parent_id":ObjectId(c_parent_val) if c_parent_val else None}
            ok,msg = validar_categoria(doc)
            if ok:
                try:
                    categorias.insert_one(doc)
                    st.success("✅ Categoría creada.")
                    st.cache_data.clear()
                except pymongo.errors.DuplicateKeyError:
                    st.error("❌ Slug duplicado (índice único).")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
            else:
                st.error(msg)

# -----------------------
# 📦 PRODUCTOS
# -----------------------
with tabs[3]:
    st.subheader("📦 Productos")
    colp1, colp2, colp3 = st.columns([2,1,1])
    with colp1: pt = st.text_input("Buscar (sku/nombre/desc.)", "", key="prod_buscar")
    with colp2: pe = st.selectbox("Estado", ["— Todos —","ACTIVO","INACTIVO","DESCONTINUADO"], key="prod_estado_filtro")
    with colp3:
        pcat_lbl = st.selectbox("Categoría", ["— Todos —"] + list(cat_map.values()), key="prod_cat_filtro")
        pcat_val = None if pcat_lbl=="— Todos —" else [k for k,v in cat_map.items() if v==pcat_lbl][0]
    filt = {}
    if pt:
        filt["$or"]=[{"sku":{"$regex":pt,"$options":"i"}},{"nombre":{"$regex":pt,"$options":"i"}},{"descripcion":{"$regex":pt,"$options":"i"}}]
    if pe!="— Todos —": filt["estado"]=pe
    if pcat_val: filt["categoria_id"]=ObjectId(pcat_val)
    rows = list(productos.find(filt).sort("nombre",1))
    df = pd.DataFrame([{
        "_id": str(r["_id"]), "SKU": r.get("sku"), "Nombre": r.get("nombre"),
        "Categoría": cat_map.get(str(r.get("categoria_id")),""),
        "Precio": r.get("precio"), "Moneda": r.get("moneda"), "Estado": r.get("estado")
    } for r in rows])
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("prod_create", clear_on_submit=True):
        sku = st.text_input("SKU", key="prod_sku")
        nombre = st.text_input("Nombre", key="prod_nombre")
        desc = st.text_area("Descripción", value="", key="prod_desc")
        c_lbl = st.selectbox("Categoría", list(cat_opts.keys()), key="prod_cat")
        c_val = cat_opts[c_lbl]
        precio = st.text_input("Precio", value="", key="prod_precio")
        moneda = st.text_input("Moneda (ISO 3)", value="PEN", key="prod_moneda")
        estado = st.selectbox("Estado", ["ACTIVO","INACTIVO","DESCONTINUADO"], key="prod_estado_create")
        if st.form_submit_button("Crear producto", use_container_width=True):
            doc = {"sku":sku.strip(),"nombre":nombre.strip(),"descripcion":desc.strip() or None,
                   "categoria_id":ObjectId(c_val) if c_val else None,
                   "atributos":None,"precio":_safe_float(precio),"moneda":moneda.strip().upper(),"estado":estado,"imagenes":[]}
            ok,msg = validar_producto(doc)
            if ok:
                try:
                    productos.insert_one(doc); st.success("✅ Producto creado."); st.cache_data.clear()
                except pymongo.errors.DuplicateKeyError:
                    st.error("❌ SKU duplicado (índice único).")
                except Exception as e:
                    st.error(f"❌ Error: {e}")
            else: st.error(msg)

# -----------------------
# 📊 INVENTARIO
# -----------------------
with tabs[4]:
    st.subheader("📊 Inventario por ubicación")
    col_i1, col_i2 = st.columns(2)
    with col_i1:
        ip_lbl = st.selectbox("Producto", list(prod_opts.keys()), key="inv_prod")
        ip_val = prod_opts[ip_lbl]
    with col_i2:
        iu_lbl = st.selectbox("Ubicación", list(ubi_opts.keys()), key="inv_ubi")
        iu_val = ubi_opts[iu_lbl]

    if ip_val:
        rows = list(inventario.find({"producto_id": ObjectId(ip_val)}))
        df = pd.DataFrame([{
            "_id": str(r["_id"]), "Producto": prod_map.get(str(r["producto_id"])),"Ubicación": ubi_map.get(str(r["ubicacion_id"])),
            "Stock": r.get("stock"), "Reservado": r.get("reservado",0), "Seguridad": r.get("seguridad"),
            "Actualizado": r.get("actualizado_en").isoformat() if r.get("actualizado_en") else ""
        } for r in rows])
        st.dataframe(df, use_container_width=True)
    else:
        st.info("Selecciona un producto para ver su stock por ubicación.")

    st.markdown("—")
    with st.form("inv_upsert", clear_on_submit=True):
        stock = st.number_input("Stock", min_value=0, value=0, key="inv_stock")
        reservado = st.number_input("Reservado", min_value=0, value=0, key="inv_reservado")
        seg = st.number_input("Stock de seguridad", min_value=0, value=0, key="inv_seg")
        if st.form_submit_button("Guardar inventario", use_container_width=True):
            _require(ip_val and iu_val, "Debes seleccionar producto y ubicación.")
            doc = {"producto_id": ObjectId(ip_val), "ubicacion_id": ObjectId(iu_val),
                   "stock": int(stock), "reservado": int(reservado), "seguridad": int(seg), "actualizado_en": dt.datetime.utcnow()}
            ok,msg = validar_inventario(doc)
            if ok:
                inventario.update_one({"producto_id":doc["producto_id"],"ubicacion_id":doc["ubicacion_id"]},{"$set":doc}, upsert=True)
                st.success("✅ Inventario guardado."); st.cache_data.clear()
            else: st.error(msg)

# -----------------------
# 👤 CLIENTES
# -----------------------
with tabs[5]:
    st.subheader("👤 Clientes")
    c_txt = st.text_input("Buscar (nombres, apellidos, doc, correo)", "", key="cli_buscar")
    f = {"$or":[{"nombres":{"$regex":c_txt,"$options":"i"}},
                {"apellidos":{"$regex":c_txt,"$options":"i"}},
                {"doc_num":{"$regex":c_txt,"$options":"i"}},
                {"correo":{"$regex":c_txt,"$options":"i"}}]} if c_txt else {}
    rows = list(clientes.find(f).sort([("apellidos",1),("nombres",1)]))
    df = pd.DataFrame([{
        "_id": str(r["_id"]),"Doc": f'{r.get("doc_tipo")}-{r.get("doc_num")}',
        "Nombre": f'{r.get("apellidos","")}, {r.get("nombres","")}',
        "Correo": r.get("correo",""),"Teléfono": r.get("telefono",""),
        "Segmento": r.get("segmento",""),"Creado": r.get("creado_en").date().isoformat() if r.get("creado_en") else ""
    } for r in rows])
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("cli_create", clear_on_submit=True):
        tdoc = st.selectbox("Tipo doc.", ["DNI","CE","PAS"], key="cli_tdoc")
        dnum = st.text_input("N° documento", key="cli_dnum")
        nom  = st.text_input("Nombres", key="cli_nombres")
        ape  = st.text_input("Apellidos", key="cli_apellidos")
        cor  = st.text_input("Correo", value="", key="cli_correo")
        tel  = st.text_input("Teléfono", value="", key="cli_tel")
        dirc = st.text_input("Dirección", value="", key="cli_dir")
        segm = st.selectbox("Segmento", ["REGULAR","VIP","NUEVO"], index=0, key="cli_segmento_create")
        if st.form_submit_button("Crear cliente", use_container_width=True):
            doc = {"doc_tipo":tdoc,"doc_num":dnum.strip(),"nombres":nom.strip(),"apellidos":ape.strip(),
                   "correo":cor.strip() or None,"telefono":tel.strip() or None,"direccion":dirc.strip() or None,
                   "segmento":segm,"creado_en": dt.datetime.utcnow()}
            ok,msg = validar_cliente(doc)
            if ok:
                try: clientes.insert_one(doc); st.success("✅ Cliente creado."); st.cache_data.clear()
                except pymongo.errors.DuplicateKeyError: st.error("❌ Cliente duplicado (índice doc_tipo+doc_num).")
                except Exception as e: st.error(f"❌ Error: {e}")
            else: st.error(msg)

# -----------------------
# 🛒 CARRITOS
# -----------------------
with tabs[6]:
    st.subheader("🛒 Carritos")
    cc_lbl = st.selectbox("Cliente", list(cli_opts.keys()), key="cart_cli")
    cc_val = cli_opts[cc_lbl]
    canal_codigo = st.selectbox("Canal", ["WEB","APP","TIENDA","DELIVERY","PICKUP"], index=0, key="cart_canal")
    if cc_val:
        cart = carritos.find_one({"cliente_id": ObjectId(cc_val), "canal_codigo": canal_codigo})
    else:
        cart = None

    st.caption("Si no existe carrito se creará al guardar.")
    n_items = st.number_input("N° de ítems", min_value=1, max_value=10, value=1, key="cart_nitems")
    items = []
    for i in range(int(n_items)):
        p_lbl = st.selectbox(f"Producto #{i+1}", list(prod_opts.keys()), key=f"cart_p_{i}")
        p_val = prod_opts[p_lbl]
        qty   = st.number_input(f"Cantidad #{i+1}", min_value=1, value=1, key=f"cart_q_{i}")
        price = float(precio_by_id.get(p_val, 0.0)) if p_val else 0.0
        st.caption(f"Precio sugerido: {price}")
        items.append({"producto_id": p_val, "cantidad": int(qty), "precio_unitario": price, "moneda":"PEN"})

    if st.button("💾 Guardar carrito", key="cart_save"):
        _require(cc_val, "Debes seleccionar un cliente.")
        if any(x["producto_id"] is None for x in items):
            st.error("❌ Todos los ítems deben tener producto.")
        else:
            for x in items:
                x["producto_id"] = ObjectId(x["producto_id"])
            doc = {"cliente_id": ObjectId(cc_val), "canal_codigo": canal_codigo,
                   "items": items, "actualizado_en": dt.datetime.utcnow()}
            ok,msg = validar_carrito(doc)
            if ok:
                carritos.update_one({"cliente_id": doc["cliente_id"],"canal_codigo": canal_codigo},{"$set": doc}, upsert=True)
                st.success("✅ Carrito guardado/actualizado."); st.cache_data.clear()
            else: st.error(msg)

    if cart:
        st.markdown("**Carrito actual**")
        cart_disp = dict(cart)
        cart_disp["_id"] = str(cart_disp["_id"])
        cart_disp["cliente_id"] = str(cart_disp["cliente_id"])
        for it in cart_disp.get("items", []):
            it["producto_id"] = str(it["producto_id"])
        st.json(cart_disp, expanded=False)

# -----------------------
# 🧾 ÓRDENES
# -----------------------
with tabs[7]:
    st.subheader("🧾 Órdenes")
    o_txt = st.text_input("Buscar por código", "", key="order_buscar")
    o_estado = st.selectbox("Estado", ["— Todos —","CREADA","PAGADA","PREPARACION","EN_RUTA","LISTA_RECOJO","ENTREGADA","CANCELADA","DEVUELTA"], key="order_estado_filter")
    filt = {}
    if o_txt: filt["codigo"]={"$regex":o_txt,"$options":"i"}
    if o_estado!="— Todos —": filt["estado"]=o_estado
    rows = list(ordenes.find(filt).sort("creada_en",-1))
    df = pd.DataFrame([{
        "Código": r.get("codigo"), "Cliente": cli_map.get(str(r.get("cliente_id")), str(r.get("cliente_id"))),
        "Canal": r.get("canal_codigo"), "Estado": r.get("estado"), "Moneda": r.get("moneda"),
        "Total": r.get("total"), "Creada": r.get("creada_en").isoformat() if r.get("creada_en") else ""
    } for r in rows])
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    st.subheader("➕ Crear orden")
    oc_lbl = st.selectbox("Cliente", list(cli_opts.keys()), key="order_cli")
    oc_val = cli_opts[oc_lbl]
    oc_canal  = st.selectbox("Canal", ["WEB","APP","TIENDA","DELIVERY","PICKUP"], index=0, key="order_canal")
    oc_moneda = st.text_input("Moneda (ISO 3)", value="PEN", key="order_moneda")
    n = st.number_input("N° de ítems", min_value=1, max_value=10, value=1, key="order_nitems")
    o_items = []
    for i in range(int(n)):
        p_lbl = st.selectbox(f"Producto #{i+1}", list(prod_opts.keys()), key=f"order_p_{i}")
        p_val = prod_opts[p_lbl]
        qty   = st.number_input(f"Cantidad #{i+1}", min_value=1, value=1, key=f"order_q_{i}")
        price = float(precio_by_id.get(p_val,0.0)) if p_val else 0.0
        st.caption(f"Precio unit. sugerido: {price}")
        o_items.append({"producto_id": p_val, "cantidad": int(qty), "precio": price})
    if st.button("Crear orden", key="order_create"):
        _require(oc_val, "Selecciona un cliente.")
        if any(x["producto_id"] is None for x in o_items):
            st.error("❌ Todos los ítems deben tener producto.")
        else:
            for x in o_items:
                x["producto_id"]=ObjectId(x["producto_id"]); x["subtotal"]=round(x["precio"]*x["cantidad"],2)
            total = round(sum(x["subtotal"] for x in o_items),2)
            codigo = f"ORD-{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            doc = {"codigo":codigo,"cliente_id":ObjectId(oc_val),"canal_codigo":oc_canal,"estado":"CREADA",
                   "fulfillment":None,"origen_ubicacion_id":None,"destino_ubicacion_id":None,
                   "items":o_items,"moneda":oc_moneda.strip().upper(),"total":total,
                   "creada_en":dt.datetime.utcnow(),"actualizada_en":dt.datetime.utcnow()}
            ok,msg = validar_orden(doc)
            if ok:
                try: ordenes.insert_one(doc); st.success(f"✅ Orden {codigo} creada."); st.cache_data.clear()
                except Exception as e: st.error(f"❌ Error: {e}")
            else: st.error(msg)

# -----------------------
# 💳 PAGOS
# -----------------------
with tabs[8]:
    st.subheader("💳 Pagos")
    ord_code = st.text_input("Código de orden", "", key="pay_code")
    o = ordenes.find_one({"codigo": ord_code}) if ord_code else None
    if o:
        st.write(f"Orden: {o.get('codigo')} — Total: {o.get('total')} {o.get('moneda')}")
        with st.form("pay_create", clear_on_submit=True):
            monto   = st.text_input("Monto", value=str(o.get("total","")), key="pay_monto")
            moneda  = st.text_input("Moneda (ISO 3)", value=o.get("moneda","PEN"), key="pay_moneda")
            metodo  = st.selectbox("Método", ["TARJETA","YAPE","PLIN","TRANSFERENCIA","EFECTIVO"], key="pay_metodo")
            estado  = st.selectbox("Estado", ["PENDIENTE","APROBADO","RECHAZADO","REEMBOLSADO"], key="pay_estado")
            if st.form_submit_button("Registrar pago", use_container_width=True):
                doc = {"orden_id": o["_id"], "monto": _safe_float(monto), "moneda": moneda.strip().upper(),
                       "metodo": metodo, "estado": estado, "transaccion_ref": f"TRX-{o.get('codigo')}",
                       "creado_en": dt.datetime.utcnow()}
                ok,msg = validar_pago(doc)
                if ok:
                    pagos.insert_one(doc); st.success("✅ Pago registrado.")
                else: st.error(msg)
    pagos_rows = list(pagos.find({"orden_id": o["_id"]}) if o else pagos.find({}).limit(50))
    df = pd.DataFrame([{
        "Orden": str(r.get("orden_id")), "Monto": r.get("monto"), "Moneda": r.get("moneda"),
        "Método": r.get("metodo"), "Estado": r.get("estado"), "Ref": r.get("transaccion_ref"),
        "Creado": r.get("creado_en").isoformat() if r.get("creado_en") else ""
    } for r in pagos_rows])
    st.dataframe(df, use_container_width=True)

# -----------------------
# 🚚 ENVÍOS
# -----------------------
with tabs[9]:
    st.subheader("🚚 Envíos")
    ord_code_e = st.text_input("Código de orden (para crear envío)", "", key="ship_code")
    o2 = ordenes.find_one({"codigo": ord_code_e}) if ord_code_e else None
    if o2:
        with st.form("ship_create", clear_on_submit=True):
            tipo      = st.selectbox("Tipo", ["DELIVERY","PICKUP"], key="ship_tipo")
            estado    = st.selectbox("Estado", ["PENDIENTE","PREPARANDO","LISTO","EN_RUTA","ENTREGADO","CANCELADO"], key="ship_estado")
            proveedor = st.text_input("Proveedor", value="Veaza Logistics", key="ship_prov")
            tracking  = st.text_input("Tracking", value=f"VL-{o2.get('codigo')}", key="ship_track")
            if st.form_submit_button("Crear envío", use_container_width=True):
                doc = {"orden_id": o2["_id"], "tipo": tipo, "estado": estado,
                       "proveedor": proveedor or None, "tracking": tracking or None,
                       "pickup_ubicacion_id": None, "ventana_retiro": None, "actualizado_en": dt.datetime.utcnow()}
                ok,msg = validar_envio(doc)
                if ok:
                    envios.update_one({"orden_id": o2["_id"]},{"$set":doc}, upsert=True)
                    st.success("✅ Envío creado/actualizado.")
                else: st.error(msg)
    env_rows = list(envios.find({}).sort("actualizado_en",-1).limit(50))
    df = pd.DataFrame([{
        "Orden": str(r.get("orden_id")), "Tipo": r.get("tipo"), "Estado": r.get("estado"),
        "Proveedor": r.get("proveedor"), "Tracking": r.get("tracking"),
        "Actualizado": r.get("actualizado_en").isoformat() if r.get("actualizado_en") else ""
    } for r in env_rows])
    st.dataframe(df, use_container_width=True)

# -----------------------
# 🏷️ PROMOCIONES
# -----------------------
with tabs[10]:
    st.subheader("🏷️ Promociones")
    txt = st.text_input("Buscar por código/desc.", "", key="promo_buscar")
    f = {"$or":[{"codigo":{"$regex":txt,"$options":"i"}},{"descripcion":{"$regex":txt,"$options":"i"}}]} if txt else {}
    rows = list(promociones.find(f).sort("codigo",1))
    df = pd.DataFrame([{
        "Código": r.get("codigo"), "Descripción": r.get("descripcion"),
        "Tipo": r.get("tipo"), "Valor": r.get("valor"), "Activo": r.get("activo")
    } for r in rows])
    st.dataframe(df, use_container_width=True)

    st.markdown("—")
    with st.form("promo_create", clear_on_submit=True):
        pc   = st.text_input("Código", key="promo_code")
        pdsc = st.text_input("Descripción", key="promo_desc")
        pt   = st.selectbox("Tipo", ["PCT_DESC","MONTO_DESC","ENVIO_GRATIS"], key="promo_tipo")
        pv   = st.text_input("Valor (opcional)", key="promo_valor")
        pact = st.checkbox("Activo", value=True, key="promo_activo")
        if st.form_submit_button("Crear promoción", use_container_width=True):
            doc = {"codigo":pc.strip(),"descripcion":pdsc.strip(),"tipo":pt,"valor": _safe_float(pv),"activo": bool(pact),
                   "aplica_sobre_skus": None,"aplica_sobre_categoria_ids": None,"vigencia": None}
            ok,msg = validar_promo(doc)
            if ok:
                try: promociones.insert_one(doc); st.success("✅ Promoción creada.")
                except pymongo.errors.DuplicateKeyError: st.error("❌ Código de promo duplicado.")
                except Exception as e: st.error(f"❌ Error: {e}")
            else: st.error(msg)

# -----------------------
# ↩️ DEVOLUCIONES
# -----------------------
with tabs[11]:
    st.subheader("↩️ Devoluciones")
    ord_code_r = st.text_input("Código de orden (para crear devolución)", "", key="ret_code")
    o3 = ordenes.find_one({"codigo": ord_code_r}) if ord_code_r else None
    n = st.number_input("N° ítems", min_value=1, max_value=10, value=1, key="ret_n")
    ret_items = []
    for i in range(int(n)):
        p_lbl = st.selectbox(f"Producto #{i+1}", list(prod_opts.keys()), key=f"ret_p_{i}")
        p_val = prod_opts[p_lbl]
        qty   = st.number_input(f"Cantidad #{i+1}", min_value=1, value=1, key=f"ret_q_{i}")
        ret_items.append({"producto_id": p_val, "cantidad": int(qty)})
    estado_r = st.selectbox("Estado", ["SOLICITADA","APROBADA","RECHAZADA","RECIBIDA","REEMBOLSADA","CERRADA"], key="ret_estado")
    if st.button("Crear devolución", key="ret_create"):
        _require(o3 is not None, "Ingresa un código de orden válido.")
        if any(x["producto_id"] is None for x in ret_items):
            st.error("❌ Todos los ítems deben tener producto.")
        else:
            for x in ret_items: x["producto_id"]=ObjectId(x["producto_id"])
            doc = {"orden_id": o3["_id"], "estado": estado_r, "motivo": None, "items": ret_items, "creada_en": dt.datetime.utcnow()}
            ok,msg = validar_devolucion(doc)
            if ok:
                devoluciones.insert_one(doc); st.success("✅ Devolución registrada.")
            else: st.error(msg)
    rows = list(devoluciones.find({}).sort("creada_en",-1).limit(50))
    df = pd.DataFrame([{
        "Orden": str(r.get("orden_id")), "Estado": r.get("estado"),
        "Items": sum(x.get("cantidad",0) for x in r.get("items",[])),
        "Creada": r.get("creada_en").isoformat() if r.get("creada_en") else ""
    } for r in rows])
    st.dataframe(df, use_container_width=True)

# -----------------------
# 📜 EVENTOS (solo lectura)
# -----------------------
with tabs[12]:
    st.subheader("📜 Eventos (auditoría)")
    tipo = st.selectbox("Tipo", ["— Todos —","ORDER_CREATED","ORDER_PAID","STOCK_UPDATED","SHIPMENT_STATUS","PROMO_APPLIED","CART_UPDATED"], key="evt_tipo")
    f = {} if tipo=="— Todos —" else {"tipo": tipo}
    rows = list(eventos.find(f).sort("timestamp",-1).limit(200))
    df = pd.DataFrame([{
        "Tipo": r.get("tipo"), "Entidad": r.get("entidad"),
        "EntidadId": str(r.get("entidad_id")), "Fecha": r.get("timestamp").isoformat() if r.get("timestamp") else "",
        "Payload": r.get("payload")
    } for r in rows])
    st.dataframe(df, use_container_width=True)

