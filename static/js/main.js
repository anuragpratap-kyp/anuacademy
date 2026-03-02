const body = document.body;
const navToggle = document.querySelector(".nav-toggle");
const navLinks = document.querySelector(".nav-links");
const navBackdrop = document.querySelector(".nav-backdrop");
let scrollYBeforeNavOpen = 0;

function lockBodyScroll() {
    scrollYBeforeNavOpen = window.scrollY || window.pageYOffset || 0;
    body.style.position = "fixed";
    body.style.top = `-${scrollYBeforeNavOpen}px`;
    body.style.left = "0";
    body.style.right = "0";
    body.style.width = "100%";
}

function unlockBodyScroll() {
    if (!body.style.position) return;
    body.style.position = "";
    body.style.top = "";
    body.style.left = "";
    body.style.right = "";
    body.style.width = "";
    window.scrollTo(0, scrollYBeforeNavOpen);
}

function closeNavMenu() {
    if (!navToggle || !navLinks || !navBackdrop) return;
    body.classList.remove("nav-open");
    unlockBodyScroll();
    navToggle.setAttribute("aria-expanded", "false");
    navBackdrop.setAttribute("aria-hidden", "true");
}

function openNavMenu() {
    if (!navToggle || !navLinks || !navBackdrop) return;
    lockBodyScroll();
    body.classList.add("nav-open");
    navToggle.setAttribute("aria-expanded", "true");
    navBackdrop.setAttribute("aria-hidden", "false");
}

if (navToggle && navLinks && navBackdrop) {
    navToggle.addEventListener("click", () => {
        if (body.classList.contains("nav-open")) {
            closeNavMenu();
            return;
        }
        openNavMenu();
    });

    navBackdrop.addEventListener("click", closeNavMenu);

    navLinks.querySelectorAll("a").forEach((link) => {
        link.addEventListener("click", closeNavMenu);
    });

    window.addEventListener("resize", () => {
        if (window.innerWidth > 768) {
            closeNavMenu();
        }
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeNavMenu();
        }
    });
}
