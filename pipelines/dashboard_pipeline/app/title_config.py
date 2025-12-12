"""
Helper function:
- configures the title section of the dashboard
- add logo to sidebar
"""
import streamlit as st
from PIL import Image


def image_background_filler(path: str) -> Image.Image:
    '''
    Helper: Fills transparent background of image to match background color.

    Args:
        path (str): Path to image file

    Returns:
        Image.Image: Image with filled background
    '''
    img = Image.open(path).convert("RGBA")
    img_with_bg = Image.new("RGB", img.size, "#e0e5ec")
    img_with_bg.paste(img, mask=img.getchannel("A"))
    return img_with_bg


def title_config(title: str):
    '''
    Adds title and logo to the dashboard and logo to sidebar.

    Args:
        title (str): The title of the dashboard
    '''
    col1, col2, col3 = st.columns([2, 7, 2])
    with col1:
        bottom_left_img = image_background_filler(
            "./title_images/bottom_left.png")
        st.image(bottom_left_img, width="stretch")

    with col2:
        st.title(title, text_alignment="center")

    with col3:
        top_right_img = image_background_filler("./title_images/top_right.png")
        st.image(top_right_img, width="stretch")

    st.logo("./title_images/logo.png", size="large")


if __name__ == "__main__":
    title_config("Test Dashboard")
    st.sidebar.header("Sidebar")
