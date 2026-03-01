const body = document.body;
const navToggle = document.querySelector(".nav-toggle");
const navLinks = document.querySelector(".nav-links");
const navBackdrop = document.querySelector(".nav-backdrop");

function closeNavMenu() {
    if (!navToggle || !navLinks || !navBackdrop) return;
    body.classList.remove("nav-open");
    navToggle.setAttribute("aria-expanded", "false");
    navBackdrop.setAttribute("aria-hidden", "true");
}

function openNavMenu() {
    if (!navToggle || !navLinks || !navBackdrop) return;
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
